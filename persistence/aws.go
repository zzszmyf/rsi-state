// Package persistence provides AWS adapters for the durable RSI state plane.
package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zzszmyf/rsi-state/evolution"
)

type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}
type ConditionalS3API interface {
	PutObjectIfAbsent(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type Artifact struct {
	URI, SHA256, ContentType string
	Size                     int64
}
type S3Artifacts struct {
	Client         S3API
	Bucket, Prefix string
}

func (s S3Artifacts) PutImmutable(ctx context.Context, name string, body []byte, contentType string) (Artifact, error) {
	if s.Client == nil || s.Bucket == "" || strings.TrimSpace(name) == "" {
		return Artifact{}, errors.New("s3 client, bucket, and object name are required")
	}
	sum := sha256.Sum256(body)
	key := path.Join(strings.Trim(s.Prefix, "/"), hex.EncodeToString(sum[:]), path.Base(name))
	conditional, ok := s.Client.(ConditionalS3API)
	if !ok {
		return Artifact{}, errors.New("S3 write-once adapter is required for immutable artifacts")
	}
	_, err := conditional.PutObjectIfAbsent(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentType: aws.String(contentType), ChecksumSHA256: aws.String(base64SHA256(sum[:]))})
	if err != nil {
		return Artifact{}, fmt.Errorf("put immutable artifact: %w", err)
	}
	return Artifact{URI: "s3://" + s.Bucket + "/" + key, SHA256: hex.EncodeToString(sum[:]), ContentType: contentType, Size: int64(len(body))}, nil
}

func (s S3Artifacts) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(strings.TrimPrefix(key, "s3://"+s.Bucket+"/"))})
	if err != nil {
		return nil, fmt.Errorf("get artifact: %w", err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	if out.ContentType == nil || strings.TrimSpace(*out.ContentType) == "" {
		return nil, fmt.Errorf("%w: artifact content type missing", evolution.ErrInvalidInput)
	}
	sum := sha256.Sum256(body)
	if out.ChecksumSHA256 == nil || *out.ChecksumSHA256 != base64SHA256(sum[:]) {
		return nil, fmt.Errorf("%w: artifact checksum mismatch", evolution.ErrInvalidInput)
	}
	return body, nil
}

// GetVerified verifies the checksum and exact content type recorded when the
// artifact was persisted. Production callers should retain Artifact metadata.
func (s S3Artifacts) GetVerified(ctx context.Context, artifact Artifact) ([]byte, error) {
	body, err := s.Get(ctx, artifact.URI)
	if err != nil {
		return nil, err
	}
	if artifact.ContentType == "" {
		return nil, fmt.Errorf("%w: expected content type is required", evolution.ErrInvalidInput)
	}
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(strings.TrimPrefix(artifact.URI, "s3://"+s.Bucket+"/"))})
	if err != nil {
		return nil, fmt.Errorf("get artifact metadata: %w", err)
	}
	defer out.Body.Close()
	if out.ContentType == nil || *out.ContentType != artifact.ContentType {
		return nil, fmt.Errorf("%w: artifact content type mismatch", evolution.ErrInvalidInput)
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != artifact.SHA256 {
		return nil, fmt.Errorf("%w: artifact checksum mismatch", evolution.ErrInvalidInput)
	}
	return body, nil
}

func base64SHA256(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

type DynamoAPI interface {
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}
type DynamoHeads struct {
	Client DynamoAPI
	Table  string
}

func (d DynamoHeads) CompareAndSwap(ctx context.Context, head evolution.AcceptedHead, expectedRevision int64) error {
	_, err := d.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.Table), Key: map[string]types.AttributeValue{"workspace_id": &types.AttributeValueMemberS{Value: head.WorkspaceID}},
		UpdateExpression:    aws.String("SET revision = :revision, event_id = :event_id, snapshot_ref = :snapshot_ref, updated_at = :updated_at"),
		ConditionExpression: aws.String("attribute_not_exists(revision) OR revision = :expected"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected":     &types.AttributeValueMemberN{Value: fmt.Sprint(expectedRevision)},
			":revision":     &types.AttributeValueMemberN{Value: fmt.Sprint(head.Revision)},
			":event_id":     &types.AttributeValueMemberS{Value: head.EventID},
			":snapshot_ref": &types.AttributeValueMemberS{Value: head.SnapshotRef},
			":updated_at":   &types.AttributeValueMemberS{Value: head.UpdatedAt.UTC().Format(time.RFC3339Nano)},
		},
	})
	if err != nil {
		return mapConditionalError(err)
	}
	return nil
}

func (d DynamoHeads) Get(ctx context.Context, workspaceID string) (evolution.AcceptedHead, error) {
	out, err := d.Client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(d.Table), Key: map[string]types.AttributeValue{"workspace_id": &types.AttributeValueMemberS{Value: workspaceID}}})
	if err != nil {
		return evolution.AcceptedHead{}, err
	}
	if len(out.Item) == 0 {
		return evolution.AcceptedHead{}, evolution.ErrNotFound
	}
	revisionValue, ok := out.Item["revision"].(*types.AttributeValueMemberN)
	if !ok {
		return evolution.AcceptedHead{}, fmt.Errorf("invalid accepted-head revision")
	}
	revision, err := strconv.ParseInt(revisionValue.Value, 10, 64)
	if err != nil {
		return evolution.AcceptedHead{}, err
	}
	eventID, _ := out.Item["event_id"].(*types.AttributeValueMemberS)
	snapshotRef, _ := out.Item["snapshot_ref"].(*types.AttributeValueMemberS)
	updatedAt, _ := out.Item["updated_at"].(*types.AttributeValueMemberS)
	head := evolution.AcceptedHead{WorkspaceID: workspaceID, Revision: revision}
	if eventID != nil {
		head.EventID = eventID.Value
	}
	if snapshotRef != nil {
		head.SnapshotRef = snapshotRef.Value
	}
	if updatedAt != nil {
		head.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt.Value)
		if err != nil {
			return evolution.AcceptedHead{}, fmt.Errorf("invalid accepted-head timestamp: %w", err)
		}
	}
	return head, nil
}

func EncodeEvent(event evolution.EventEnvelope) ([]byte, error) { return json.Marshal(event) }

// S3ObjectAPI is the S3 operation subset used by S3ObjectStore.
type S3ObjectAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// S3ObjectStore adapts an S3 bucket to DurableStore's ObjectStore interface.
type S3ObjectStore struct {
	Client S3ObjectAPI
	Bucket string
}

// ConditionalS3ObjectAPI is the adapter boundary for write-once object
// creation. Current AWS SDK versions used by this module predate PutObject's
// If-None-Match field, so production adapters must map this to S3 conditional
// creation or a bucket policy that rejects overwrites.
func (s S3ObjectStore) Put(ctx context.Context, key string, body []byte, contentType string) error {
	conditional, ok := s.Client.(ConditionalS3API)
	if !ok {
		return errors.New("S3 write-once adapter is required for durable records")
	}
	sum := sha256.Sum256(body)
	_, err := conditional.PutObjectIfAbsent(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.key(key)), Body: bytes.NewReader(body), ContentType: aws.String(contentType), ChecksumSHA256: aws.String(base64SHA256(sum[:]))})
	if err != nil {
		return mapConditionalError(err)
	}
	return err
}
func (s S3ObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.key(key))})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	if out.ContentType == nil || strings.TrimSpace(*out.ContentType) == "" {
		return nil, fmt.Errorf("%w: object content type missing", evolution.ErrInvalidInput)
	}
	sum := sha256.Sum256(body)
	if out.ChecksumSHA256 == nil || *out.ChecksumSHA256 != base64SHA256(sum[:]) {
		return nil, fmt.Errorf("%w: object checksum mismatch", evolution.ErrInvalidInput)
	}
	return body, nil
}
func (s S3ObjectStore) List(ctx context.Context, prefix string) ([]string, error) {
	out, err := s.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.Bucket), Prefix: aws.String(s.key(prefix))})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(out.Contents))
	for _, object := range out.Contents {
		if object.Key != nil {
			keys = append(keys, *object.Key)
		}
	}
	return keys, nil
}
func (s S3ObjectStore) key(key string) string {
	key = strings.TrimPrefix(key, "s3://"+s.Bucket+"/")
	return strings.TrimPrefix(key, "/")
}

// Create initializes a head only if no accepted-head record exists.
func (d DynamoHeads) Create(ctx context.Context, head evolution.AcceptedHead) error {
	_, err := d.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(d.Table), Key: map[string]types.AttributeValue{"workspace_id": &types.AttributeValueMemberS{Value: head.WorkspaceID}},
		UpdateExpression:    aws.String("SET revision = :revision, event_id = :event_id, snapshot_ref = :snapshot_ref, updated_at = :updated_at"),
		ConditionExpression: aws.String("attribute_not_exists(workspace_id)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":revision": &types.AttributeValueMemberN{Value: fmt.Sprint(head.Revision)}, ":event_id": &types.AttributeValueMemberS{Value: head.EventID}, ":snapshot_ref": &types.AttributeValueMemberS{Value: head.SnapshotRef}, ":updated_at": &types.AttributeValueMemberS{Value: head.UpdatedAt.UTC().Format(time.RFC3339Nano)},
		},
	})
	if err != nil {
		return mapConditionalError(err)
	}
	return nil
}

func mapConditionalError(err error) error {
	var conditional *types.ConditionalCheckFailedException
	if errors.As(err, &conditional) {
		return fmt.Errorf("%w: %v", evolution.ErrRevisionConflict, err)
	}
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "ConditionalCheckFailedException" || apiErr.ErrorCode() == "PreconditionFailed") {
		return fmt.Errorf("%w: %v", evolution.ErrRevisionConflict, err)
	}
	return err
}

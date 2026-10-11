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

type Artifact struct {
	URI, SHA256 string
	Size        int64
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
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentType: aws.String(contentType), ChecksumSHA256: aws.String(base64SHA256(sum[:]))})
	if err != nil {
		return Artifact{}, fmt.Errorf("put immutable artifact: %w", err)
	}
	return Artifact{URI: "s3://" + s.Bucket + "/" + key, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))}, nil
}

func (s S3Artifacts) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(strings.TrimPrefix(key, "s3://"+s.Bucket+"/"))})
	if err != nil {
		return nil, fmt.Errorf("get artifact: %w", err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
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
		return fmt.Errorf("conditional accepted-head update: %w", err)
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
	revision, err := strconv.ParseInt(out.Item["revision"].(*types.AttributeValueMemberN).Value, 10, 64)
	if err != nil {
		return evolution.AcceptedHead{}, err
	}
	return evolution.AcceptedHead{WorkspaceID: workspaceID, Revision: revision, EventID: out.Item["event_id"].(*types.AttributeValueMemberS).Value, SnapshotRef: out.Item["snapshot_ref"].(*types.AttributeValueMemberS).Value}, nil
}

func EncodeEvent(event evolution.EventEnvelope) ([]byte, error) { return json.Marshal(event) }

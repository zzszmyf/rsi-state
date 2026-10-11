package persistence

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zzszmyf/rsi-state/evolution"
)

type fakeS3 struct {
	input *s3.PutObjectInput
	body  string
}

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.input = in
	return &s3.PutObjectOutput{}, nil
}
func (f *fakeS3) PutObjectIfAbsent(ctx context.Context, in *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return f.PutObject(ctx, in, options...)
}
func (f *fakeS3) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	body := f.body
	if body == "" {
		body = "artifact"
	}
	sum := sha256.Sum256([]byte(body))
	contentType := "application/json"
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(body)), ContentType: &contentType, ChecksumSHA256: aws.String(base64SHA256(sum[:]))}, nil
}

func TestPutImmutableAddressesArtifactByHash(t *testing.T) {
	client := &fakeS3{}
	artifact, err := (S3Artifacts{Client: client, Bucket: "bucket", Prefix: "rsi"}).PutImmutable(context.Background(), "event.json", []byte("hello"), "application/json")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(artifact.URI, artifact.SHA256) {
		t.Fatalf("URI %q does not contain hash %q", artifact.URI, artifact.SHA256)
	}
	if got := *client.input.Bucket; got != "bucket" {
		t.Fatalf("bucket = %q", got)
	}
}

func TestGetVerifiedRejectsChecksumOrContentTypeMismatch(t *testing.T) {
	client := &fakeS3{}
	artifact := Artifact{URI: "s3://bucket/rsi/hash/event.json", SHA256: strings.Repeat("0", 64), ContentType: "application/json"}
	if _, err := (S3Artifacts{Client: client, Bucket: "bucket"}).GetVerified(context.Background(), artifact); !errors.Is(err, evolution.ErrInvalidInput) {
		t.Fatalf("GetVerified() checksum error = %v", err)
	}
}

type fakeDynamo struct{ input *dynamodb.UpdateItemInput }

func (f *fakeDynamo) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.input = in
	return &dynamodb.UpdateItemOutput{}, nil
}
func (f *fakeDynamo) GetItem(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func TestDynamoHeadUsesConditionalRevisionUpdate(t *testing.T) {
	client := &fakeDynamo{}
	err := (DynamoHeads{Client: client, Table: "heads"}).CompareAndSwap(context.Background(), evolution.AcceptedHead{WorkspaceID: "arc", Revision: 1, EventID: "evt", SnapshotRef: "s3://x"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := *client.input.ConditionExpression; !strings.Contains(got, "revision = :expected") {
		t.Fatalf("condition = %q", got)
	}
}

/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const awsEndpoint = "s3.amazonaws.com"

// s3Store implements Store on top of an S3-compatible bucket.
type s3Store struct {
	client *minio.Client
	bucket string
	prefix string
}

// NewS3 returns a Store for the archive described by cfg.
func NewS3(cfg Config) (Store, error) {
	bucket, prefix, err := ParseDestination(cfg.DestinationPath)
	if err != nil {
		return nil, err
	}

	host, secure := awsEndpoint, true
	lookup := minio.BucketLookupAuto
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("invalid endpoint %q", cfg.Endpoint)
		}
		host, secure = u.Host, u.Scheme == "https"
		// Like Litestream, use path-style addressing for custom endpoints:
		// most S3-compatible stores don't serve virtual-hosted buckets.
		lookup = minio.BucketLookupPath
	}

	client, err := minio.New(host, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure:       secure,
		Region:       cfg.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}
	return &s3Store{client: client, bucket: bucket, prefix: prefix}, nil
}

func (s *s3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, Join(s.prefix, key), bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("failed to put %s: %w", key, err)
	}
	return nil
}

func (s *s3Store) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	opts.SetMatchETagExcept("*")
	_, err := s.client.PutObject(ctx, s.bucket, Join(s.prefix, key), bytes.NewReader(data), int64(len(data)), opts)
	switch resp := minio.ToErrorResponse(err); {
	case err == nil:
		return nil
	case resp.StatusCode == http.StatusPreconditionFailed || resp.Code == "PreconditionFailed":
		return fmt.Errorf("%s: %w", key, ErrExists)
	case resp.StatusCode == http.StatusNotImplemented || resp.Code == "NotImplemented":
		// Conditional writes are not supported by every S3-compatible store.
		return s.Put(ctx, key, data)
	default:
		return fmt.Errorf("failed to put %s: %w", key, err)
	}
}

func (s *s3Store) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, Join(s.prefix, key), minio.GetObjectOptions{})
	if err != nil {
		return nil, s.wrapErr(key, err)
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, s.wrapErr(key, err)
	}
	return data, nil
}

func (s *s3Store) List(ctx context.Context, prefix string) ([]Object, error) {
	// Join drops slashes, but a trailing one matters for prefix matching
	// ("db/" must not match "db2/"), so restore it.
	full := Join(s.prefix, prefix)
	if full != "" && (prefix == "" || strings.HasSuffix(prefix, "/")) {
		full += "/"
	}
	root := ""
	if s.prefix != "" {
		root = s.prefix + "/"
	}

	var objects []Object
	for info := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: full, Recursive: true}) {
		if info.Err != nil {
			return nil, fmt.Errorf("failed to list %s: %w", prefix, info.Err)
		}
		objects = append(objects, Object{
			Key:          strings.TrimPrefix(info.Key, root),
			Size:         info.Size,
			LastModified: info.LastModified,
		})
	}
	return objects, nil
}

func (s *s3Store) wrapErr(key string, err error) error {
	if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
		return fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	var resp minio.ErrorResponse
	if errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%s: %w", key, ErrNotFound)
	}
	return fmt.Errorf("failed to get %s: %w", key, err)
}

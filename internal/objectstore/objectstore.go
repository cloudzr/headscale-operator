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

// Package objectstore is a minimal object storage abstraction used by the
// operator and the backup-agent to read and write a backup archive. Keys are
// always relative to the archive root (the prefix of destination_path), so
// callers never deal with bucket names.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrNotFound is returned by Get when the key does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrExists is returned by PutIfAbsent when the key already exists.
	ErrExists = errors.New("object already exists")
)

// Object describes a stored object.
type Object struct {
	// Key is relative to the archive root.
	Key          string
	Size         int64
	LastModified time.Time
}

// Store reads and writes objects below an archive root.
type Store interface {
	// Put writes data to key, replacing any existing object.
	Put(ctx context.Context, key string, data []byte) error
	// PutIfAbsent writes data to key only if no object exists there,
	// returning ErrExists otherwise. Stores without conditional writes fall
	// back to a plain Put; callers must verify by reading the key back.
	PutIfAbsent(ctx context.Context, key string, data []byte) error
	// Get reads key, returning ErrNotFound if it does not exist.
	Get(ctx context.Context, key string) ([]byte, error)
	// List returns every object whose key starts with prefix, recursively.
	List(ctx context.Context, prefix string) ([]Object, error)
}

// Config identifies an archive and how to authenticate against it.
type Config struct {
	// DestinationPath is s3://<bucket>[/<prefix>].
	DestinationPath string
	// Endpoint is an optional S3-compatible endpoint URL (http or https).
	Endpoint string
	Region   string
	// AccessKeyID and SecretAccessKey are the static credentials.
	AccessKeyID     string
	SecretAccessKey string
}

// ParseDestination splits s3://bucket/prefix into its bucket and prefix. The
// returned prefix has no leading or trailing slash.
func ParseDestination(path string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(path, "s3://")
	if !ok {
		return "", "", fmt.Errorf("destination %q must start with s3://", path)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("destination %q has no bucket", path)
	}
	return bucket, strings.Trim(prefix, "/"), nil
}

// Join builds an object key from path segments, ignoring empty ones.
func Join(parts ...string) string {
	nonEmpty := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.Trim(p, "/"); p != "" {
			nonEmpty = append(nonEmpty, p)
		}
	}
	return strings.Join(nonEmpty, "/")
}

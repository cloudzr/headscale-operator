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

package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/infradohq/headscale-operator/internal/objectstore"
)

// KeyFile is a private key file on the headscale data volume.
type KeyFile struct {
	Role string
	Path string
}

// ParseKeyFile parses a "role=path" flag value.
func ParseKeyFile(s string) (KeyFile, error) {
	role, path, ok := strings.Cut(s, "=")
	if !ok || role == "" || path == "" {
		return KeyFile{}, fmt.Errorf("invalid key %q, expected role=path", s)
	}
	return KeyFile{Role: role, Path: path}, nil
}

// KeyArchiver uploads headscale's private keys to the archive. Headscale
// generates the keys on first start and never rotates them, so the archiver
// only uploads a key when its checksum differs from the manifest.
type KeyArchiver struct {
	Store objectstore.Store
	Keys  []KeyFile
	Log   logr.Logger
	// Now defaults to time.Now.
	Now func() time.Time
	// Reporter, when set, writes the replication heartbeat on every tick.
	Reporter *ReplicationReporter

	manifest *Manifest
}

// Sync uploads every key that exists locally and is not archived with the
// same checksum. It returns true once all keys are archived.
func (a *KeyArchiver) Sync(ctx context.Context) (bool, error) {
	if a.manifest == nil {
		m, err := ReadManifest(ctx, a.Store)
		if err != nil {
			return false, err
		}
		if m == nil {
			m = &Manifest{Keys: map[string]KeyEntry{}}
		}
		a.manifest = m
	}

	done := true
	for _, key := range a.Keys {
		data, err := os.ReadFile(key.Path)
		if errors.Is(err, fs.ErrNotExist) {
			// Headscale has not generated this key yet.
			done = false
			continue
		}
		if err != nil {
			return false, fmt.Errorf("failed to read %s key: %w", key.Role, err)
		}
		sum := sha256Hex(data)
		if entry, ok := a.manifest.Keys[key.Role]; ok && entry.SHA256 == sum {
			continue
		}

		// Upload the key before the manifest so the manifest never
		// references a key that is not in the archive.
		if err := a.Store.Put(ctx, KeyObject(key.Role), data); err != nil {
			return false, err
		}
		a.manifest.Keys[key.Role] = KeyEntry{SHA256: sum, Size: int64(len(data)), UploadedAt: a.now().UTC()}
		if err := WriteManifest(ctx, a.Store, a.manifest); err != nil {
			// Force a manifest re-read next time; the key upload is harmless.
			a.manifest = nil
			return false, err
		}
		a.Log.Info("Archived private key", "role", key.Role, "sha256", sum)
	}
	return done, nil
}

// Run calls Sync, then the Reporter, every interval until ctx is cancelled.
// Errors are logged and retried: a temporarily unreachable object store must
// never stop the agent.
func (a *KeyArchiver) Run(ctx context.Context, interval time.Duration) {
	archived := false
	for {
		done, err := a.Sync(ctx)
		switch {
		case err != nil:
			a.Log.Error(err, "Failed to archive private keys")
		case done && !archived:
			a.Log.Info("All private keys archived")
		case !done:
			a.Log.V(1).Info("Waiting for headscale to generate private keys")
		}
		archived = err == nil && done

		if a.Reporter != nil {
			if _, err := a.Reporter.Report(ctx); err != nil {
				a.Log.Error(err, "Failed to write the replication heartbeat")
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func (a *KeyArchiver) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

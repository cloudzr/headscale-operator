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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"

	"github.com/infradohq/headscale-operator/internal/objectstore"
)

const (
	// StagedSuffix is appended to the database path for the copy Litestream
	// restores before the Restorer moves it into place.
	StagedSuffix = ".restoring"
	// RestoredMarkerFile is written next to the database once a recovery has
	// completed, which makes re-running a finished recovery a no-op.
	RestoredMarkerFile = ".headscale-operator-restored"
)

var (
	// ErrDatabaseExists means the target volume already holds a database that
	// this recovery did not produce. Recovery never overwrites data.
	ErrDatabaseExists = errors.New("a database already exists on the volume")
	// ErrKeysMissing means the archive lacks a key required for recovery.
	ErrKeysMissing = errors.New("archive is missing a private key required for recovery")
)

// Restorer completes a recovery on the headscale data volume. Litestream has
// already restored the database to DBPath+StagedSuffix; the Restorer fetches
// and verifies the private keys, then moves everything into place with the
// database last, so headscale never sees a partially recovered volume.
type Restorer struct {
	Store  objectstore.Store
	Keys   []KeyFile
	DBPath string
	// Source describes the recovery source in the completion marker.
	Source string
	Log    logr.Logger
}

// Restore runs the recovery. It is idempotent: it succeeds without changes
// when a previous run already completed.
func (r *Restorer) Restore(ctx context.Context) error {
	dir := filepath.Dir(r.DBPath)
	staged := r.DBPath + StagedSuffix

	if exists(r.DBPath) {
		if exists(filepath.Join(dir, RestoredMarkerFile)) {
			// A re-run Litestream restore may have staged another copy.
			for _, suffix := range []string{"", "-wal", "-shm"} {
				if err := os.Remove(staged + suffix); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return err
				}
			}
			r.Log.Info("Recovery already completed", "path", r.DBPath)
			return nil
		}
		return fmt.Errorf("%w: %s", ErrDatabaseExists, r.DBPath)
	}
	if !exists(staged) {
		return fmt.Errorf("restored database %s not found: litestream restore did not complete", staged)
	}

	manifest, err := ReadManifest(ctx, r.Store)
	if err != nil {
		return err
	}

	// Stage every key next to its destination after verifying it.
	var moves [][2]string
	for _, key := range r.Keys {
		entry, ok := KeyEntry{}, false
		if manifest != nil {
			entry, ok = manifest.Keys[key.Role]
		}
		if !ok {
			if RequiredForRecovery(key.Role) {
				return fmt.Errorf("%w: %s", ErrKeysMissing, key.Role)
			}
			r.Log.Info("Private key not in archive; headscale will generate a new one", "role", key.Role)
			continue
		}

		data, err := r.Store.Get(ctx, KeyObject(key.Role))
		if err != nil {
			return fmt.Errorf("failed to download %s key: %w", key.Role, err)
		}
		if got := sha256Hex(data); got != entry.SHA256 {
			return fmt.Errorf("%s key checksum mismatch: archive manifest has %s, downloaded %s", key.Role, entry.SHA256, got)
		}
		tmp := key.Path + StagedSuffix
		if err := writeFileSync(tmp, data, 0o600); err != nil {
			return fmt.Errorf("failed to stage %s key: %w", key.Role, err)
		}
		moves = append(moves, [2]string{tmp, key.Path})
	}
	restoredKeys := len(moves)

	// The database goes last: its presence is what marks the volume as
	// initialized, for headscale and for this function on a retry.
	for _, suffix := range []string{"-wal", "-shm"} {
		if exists(staged + suffix) {
			moves = append(moves, [2]string{staged + suffix, r.DBPath + suffix})
		}
	}
	moves = append(moves, [2]string{staged, r.DBPath})

	for _, m := range moves {
		if err := os.Rename(m[0], m[1]); err != nil {
			return fmt.Errorf("failed to move %s into place: %w", m[1], err)
		}
	}

	marker, err := json.Marshal(map[string]string{
		"source":     r.Source,
		"restoredAt": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	if err := writeFileSync(filepath.Join(dir, RestoredMarkerFile), marker, 0o600); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	r.Log.Info("Recovery completed", "path", r.DBPath, "keys", restoredKeys)
	return nil
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, fs.ErrNotExist)
}

func writeFileSync(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

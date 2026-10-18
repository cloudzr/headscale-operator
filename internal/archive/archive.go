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

// Package archive defines the layout of a Headscale backup archive and the
// operations on it shared by the operator and the backup-agent:
//
//	<root>/db/...                    Litestream replica of the SQLite database
//	<root>/keys/<role>.key           headscale private keys (noise, derp)
//	<root>/keys/manifest.json        checksums of the archived keys
//	<root>/agent/heartbeat.json      replication state reported by the backup-agent
//	<root>/.headscale-operator.json  marker naming the instance that owns the archive
package archive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/infradohq/headscale-operator/internal/objectstore"
)

const (
	// DBPrefix is where Litestream replicates the database, relative to the root.
	DBPrefix = "db"
	// ManifestKey records the checksum of every archived key.
	ManifestKey = "keys/manifest.json"
	// MarkerKey names the Headscale instance that writes to the archive.
	MarkerKey = ".headscale-operator.json"
	// keysPrefix holds the archived private keys and their manifest.
	keysPrefix = "keys/"

	// snapshotPrefix is where Litestream v0.5 stores full snapshots (the
	// highest compaction level). A recovery needs at least one of them.
	snapshotPrefix = DBPrefix + "/0009/"
)

// Key roles. The noise key is the server's identity: without it every client
// must re-register, so it is required for recovery. The DERP key is
// re-advertised to clients through the DERP map and may be regenerated.
const (
	RoleNoise = "noise"
	RoleDERP  = "derp"
)

// ErrArchiveInUse is returned when another instance owns the archive.
var ErrArchiveInUse = errors.New("archive is owned by another Headscale instance")

// KeyObject returns the object key holding the private key of role.
func KeyObject(role string) string { return keysPrefix + role + ".key" }

// RequiredForRecovery reports whether a recovery must fail without this key.
func RequiredForRecovery(role string) bool { return role == RoleNoise }

// Manifest lists the archived keys.
type Manifest struct {
	Keys map[string]KeyEntry `json:"keys"`
}

// KeyEntry describes one archived key.
type KeyEntry struct {
	SHA256     string    `json:"sha256"`
	Size       int64     `json:"size"`
	UploadedAt time.Time `json:"uploadedAt"`
}

// Marker identifies the Headscale instance that writes to an archive.
type Marker struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       string    `json:"uid"`
	CreatedAt time.Time `json:"createdAt"`
}

func (m Marker) String() string { return m.Namespace + "/" + m.Name + " (uid " + m.UID + ")" }

// ReadManifest returns the archive's key manifest, or nil if there is none.
func ReadManifest(ctx context.Context, store objectstore.Store) (*Manifest, error) {
	m := &Manifest{}
	found, err := readJSON(ctx, store, ManifestKey, m)
	if err != nil || !found {
		return nil, err
	}
	if m.Keys == nil {
		m.Keys = map[string]KeyEntry{}
	}
	return m, nil
}

// WriteManifest replaces the archive's key manifest.
func WriteManifest(ctx context.Context, store objectstore.Store, m *Manifest) error {
	return writeJSON(ctx, store, ManifestKey, m)
}

// ReadMarker returns the archive's owner marker, or nil if there is none.
func ReadMarker(ctx context.Context, store objectstore.Store) (*Marker, error) {
	m := &Marker{}
	found, err := readJSON(ctx, store, MarkerKey, m)
	if err != nil || !found {
		return nil, err
	}
	return m, nil
}

// Claim makes owner the writer of the archive. It succeeds when the archive is
// unclaimed (writing the marker) or already owned by owner.UID, and returns
// ErrArchiveInUse when another instance owns it. This is what prevents two
// live instances, or a recovered instance and its source, from interleaving
// writes in one archive.
//
// The marker is written with a conditional put, so of two instances racing
// for an unclaimed archive only one wins. It is then read back, which also
// settles the race on stores that don't support conditional writes.
func Claim(ctx context.Context, store objectstore.Store, owner Marker) error {
	current, err := ReadMarker(ctx, store)
	if err != nil {
		return err
	}
	if current == nil {
		data, err := json.MarshalIndent(owner, "", "  ")
		if err != nil {
			return err
		}
		if err := store.PutIfAbsent(ctx, MarkerKey, data); err != nil && !errors.Is(err, objectstore.ErrExists) {
			return err
		}
		if current, err = ReadMarker(ctx, store); err != nil {
			return err
		}
		if current == nil {
			return fmt.Errorf("%s not found after writing it", MarkerKey)
		}
	}
	if current.UID != owner.UID {
		return fmt.Errorf("%w: %s", ErrArchiveInUse, current)
	}
	return nil
}

// Inspection summarizes the contents of an archive.
type Inspection struct {
	// HasSnapshot reports whether a full database snapshot exists.
	HasSnapshot bool
	// HasDatabase reports whether any database replica object exists.
	HasDatabase bool
	// FirstRecoverabilityPoint is the time of the oldest snapshot.
	FirstRecoverabilityPoint *time.Time
	// LastBackup is the time of the newest database object.
	LastBackup *time.Time
	Manifest   *Manifest
	Marker     *Marker
	// Heartbeat is the backup-agent's latest report, or nil.
	Heartbeat *Heartbeat
	// keySizes maps the key objects present in the archive to their size.
	keySizes map[string]int64
}

// MissingKeys returns the roles that are not archived: absent from the
// manifest, or whose key object is missing or doesn't match the manifest's
// size (deleted by a bucket lifecycle rule, for example).
func (i Inspection) MissingKeys(roles []string) []string {
	var missing []string
	for _, role := range roles {
		if !keyArchived(i.Manifest, i.keySizes, role) {
			missing = append(missing, role)
		}
	}
	return missing
}

// keyArchived reports whether the manifest lists role and its key object is
// in the archive with the recorded size.
func keyArchived(m *Manifest, keySizes map[string]int64, role string) bool {
	if m == nil {
		return false
	}
	entry, ok := m.Keys[role]
	if !ok {
		return false
	}
	size, ok := keySizes[KeyObject(role)]
	return ok && size == entry.Size
}

// listKeySizes returns the size of every object under keys/.
func listKeySizes(ctx context.Context, store objectstore.Store) (map[string]int64, error) {
	objects, err := store.List(ctx, keysPrefix)
	if err != nil {
		return nil, err
	}
	sizes := make(map[string]int64, len(objects))
	for _, obj := range objects {
		sizes[obj.Key] = obj.Size
	}
	return sizes, nil
}

// Inspect lists the archive and summarizes what can be recovered from it.
func Inspect(ctx context.Context, store objectstore.Store) (Inspection, error) {
	var in Inspection
	objects, err := store.List(ctx, DBPrefix+"/")
	if err != nil {
		return in, err
	}
	for _, obj := range objects {
		in.HasDatabase = true
		mod := obj.LastModified
		if in.LastBackup == nil || mod.After(*in.LastBackup) {
			in.LastBackup = &mod
		}
		if strings.HasPrefix(obj.Key, snapshotPrefix) {
			in.HasSnapshot = true
			if in.FirstRecoverabilityPoint == nil || mod.Before(*in.FirstRecoverabilityPoint) {
				in.FirstRecoverabilityPoint = &mod
			}
		}
	}
	if in.Manifest, err = ReadManifest(ctx, store); err != nil {
		return in, err
	}
	if in.keySizes, err = listKeySizes(ctx, store); err != nil {
		return in, err
	}
	if in.Marker, err = ReadMarker(ctx, store); err != nil {
		return in, err
	}
	if in.Heartbeat, err = ReadHeartbeat(ctx, store); err != nil {
		return in, err
	}
	return in, nil
}

func readJSON(ctx context.Context, store objectstore.Store, key string, into any) (bool, error) {
	data, err := store.Get(ctx, key)
	if errors.Is(err, objectstore.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, into); err != nil {
		return false, fmt.Errorf("failed to decode %s: %w", key, err)
	}
	return true, nil
}

func writeJSON(ctx context.Context, store objectstore.Store, key string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return store.Put(ctx, key, data)
}

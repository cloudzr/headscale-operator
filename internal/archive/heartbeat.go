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
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/infradohq/headscale-operator/internal/objectstore"
)

// HeartbeatKey holds the backup-agent's latest replication report.
const HeartbeatKey = "agent/heartbeat.json"

// litestreamTXIDMetric is Litestream's position in the local database.
const litestreamTXIDMetric = "litestream_txid"

// Heartbeat is what the backup-agent reports from inside the headscale pod,
// with the pod's own credentials. A recent heartbeat proves the pod can still
// write to the archive. LaggingSince tells whether Litestream keeps up: an
// idle database is not lagging, so it never raises a false alarm.
type Heartbeat struct {
	At time.Time `json:"at"`
	// LocalTXID is Litestream's position in the local database, 0 when
	// Litestream has not opened the database yet.
	LocalTXID uint64 `json:"localTXID"`
	// ArchivedTXID is the highest transaction in the archive.
	ArchivedTXID uint64 `json:"archivedTXID"`
	// LaggingSince is when the archive fell behind the local database
	// without catching up since.
	LaggingSince *time.Time `json:"laggingSince,omitempty"`
	// Error explains why the local position could not be read.
	Error string `json:"error,omitempty"`
}

// ReadHeartbeat returns the archive's heartbeat, or nil if there is none.
func ReadHeartbeat(ctx context.Context, store objectstore.Store) (*Heartbeat, error) {
	hb := &Heartbeat{}
	found, err := readJSON(ctx, store, HeartbeatKey, hb)
	if err != nil || !found {
		return nil, err
	}
	return hb, nil
}

// ArchivedTXID returns the highest transaction ID among the Litestream LTX
// files (<min>-<max>.ltx, hexadecimal) in objects.
func ArchivedTXID(objects []objectstore.Object) uint64 {
	var highest uint64
	for _, obj := range objects {
		name, ok := strings.CutSuffix(path.Base(obj.Key), ".ltx")
		if !ok {
			continue
		}
		_, maxTXID, ok := strings.Cut(name, "-")
		if !ok {
			continue
		}
		if txid, err := strconv.ParseUint(maxTXID, 16, 64); err == nil && txid > highest {
			highest = txid
		}
	}
	return highest
}

// ReplicationReporter compares Litestream's local position with the archive
// and writes the result as the archive's heartbeat.
type ReplicationReporter struct {
	Store objectstore.Store
	// MetricsURL is Litestream's Prometheus endpoint in the same pod.
	MetricsURL string
	// DBPath selects the database in Litestream's metrics.
	DBPath string
	// Client defaults to a client with a short timeout.
	Client *http.Client
	// Now defaults to time.Now.
	Now func() time.Time

	laggingSince *time.Time
}

// Report writes a heartbeat. An error means the heartbeat could not be
// written, which the operator notices as a stale heartbeat.
func (r *ReplicationReporter) Report(ctx context.Context) (Heartbeat, error) {
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	hb := Heartbeat{At: now().UTC()}

	objects, err := r.Store.List(ctx, DBPrefix+"/")
	if err != nil {
		return hb, err
	}
	hb.ArchivedTXID = ArchivedTXID(objects)

	local, err := r.localTXID(ctx)
	if err != nil {
		hb.Error = err.Error()
	} else {
		hb.LocalTXID = local
		switch {
		case local <= hb.ArchivedTXID:
			r.laggingSince = nil
		case r.laggingSince == nil:
			at := hb.At
			r.laggingSince = &at
		}
	}
	hb.LaggingSince = r.laggingSince
	return hb, writeJSON(ctx, r.Store, HeartbeatKey, hb)
}

// localTXID reads litestream_txid for DBPath from Litestream's metrics. It
// returns 0 when the series doesn't exist yet (database not created).
func (r *ReplicationReporter) localTXID(ctx context.Context) (uint64, error) {
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.MetricsURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to read Litestream metrics: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to read Litestream metrics: %s", resp.Status)
	}
	return parseTXID(resp.Body, r.DBPath)
}

// parseTXID extracts litestream_txid{db="<dbPath>"} from the Prometheus text
// format. When the label doesn't match, a single series is used as is.
func parseTXID(body io.Reader, dbPath string) (uint64, error) {
	var (
		values  []float64
		matched *float64
	)
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		rest, ok := strings.CutPrefix(line, litestreamTXIDMetric)
		if !ok || (rest != "" && rest[0] != '{' && rest[0] != ' ') {
			continue
		}
		labels := ""
		if strings.HasPrefix(rest, "{") {
			end := strings.Index(rest, "}")
			if end < 0 {
				continue
			}
			labels, rest = rest[1:end], rest[end+1:]
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %s value %q", litestreamTXIDMetric, fields[0])
		}
		values = append(values, v)
		if strings.Contains(labels, `db="`+dbPath+`"`) {
			matched = &values[len(values)-1]
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	switch {
	case matched != nil:
		return uint64(*matched), nil
	case len(values) == 1:
		return uint64(values[0]), nil
	default:
		return 0, nil
	}
}

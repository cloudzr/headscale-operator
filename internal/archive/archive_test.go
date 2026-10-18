package archive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/infradohq/headscale-operator/internal/objectstore"
)

const rivalUID = "uid-2"

var _ = Describe("Claim", func() {
	ctx := context.Background()
	owner := Marker{Namespace: "ns", Name: "hs", UID: "uid-1"}

	It("claims an unowned archive", func() {
		store := &objectstore.Memory{}
		Expect(Claim(ctx, store, owner)).To(Succeed())
		m, err := ReadMarker(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(m.UID).To(Equal("uid-1"))
	})

	It("is a no-op for the current owner", func() {
		store := &objectstore.Memory{}
		Expect(Claim(ctx, store, owner)).To(Succeed())
		Expect(Claim(ctx, store, owner)).To(Succeed())
		Expect(store.Puts[MarkerKey]).To(Equal(1))
	})

	It("refuses an archive owned by another instance", func() {
		store := &objectstore.Memory{}
		Expect(Claim(ctx, store, Marker{Namespace: "ns", Name: "other", UID: rivalUID})).To(Succeed())
		err := Claim(ctx, store, owner)
		Expect(errors.Is(err, ErrArchiveInUse)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("ns/other"))
	})

	It("surfaces object store errors", func() {
		store := &objectstore.Memory{Err: errors.New("unreachable")}
		Expect(Claim(ctx, store, owner)).To(MatchError(ContainSubstring("unreachable")))
	})

	It("loses a race against an instance that claimed in between", func() {
		store := &racingStore{Memory: &objectstore.Memory{}, rival: Marker{Namespace: "ns", Name: "rival", UID: rivalUID}}
		err := Claim(ctx, store, owner)
		Expect(errors.Is(err, ErrArchiveInUse)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("ns/rival"))
		m, err := ReadMarker(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(m.UID).To(Equal(rivalUID), "the rival's marker is kept")
	})

	It("loses to a rival that overwrote the marker on a store ignoring conditional writes", func() {
		store := &blindStore{Memory: &objectstore.Memory{}, rival: Marker{Namespace: "ns", Name: "rival", UID: rivalUID}}
		err := Claim(ctx, store, owner)
		Expect(errors.Is(err, ErrArchiveInUse)).To(BeTrue(), "the first read-back still showed our marker")
		Expect(err.Error()).To(ContainSubstring("ns/rival"))
	})
})

// blindStore ignores conditional writes, like some S3-compatible stores, and
// lets rival overwrite the marker right after the first read-back.
type blindStore struct {
	*objectstore.Memory
	rival  Marker
	reads  int
	writes int
}

func (s *blindStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	s.writes++
	return s.Put(ctx, key, data)
}

func (s *blindStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := s.Memory.Get(ctx, key)
	if key == MarkerKey && s.writes > 0 {
		if s.reads++; s.reads == 1 {
			Expect(writeJSON(ctx, s.Memory, MarkerKey, s.rival)).To(Succeed())
		}
	}
	return data, err
}

// racingStore claims the archive for rival right before the first
// conditional write, as a concurrent instance would.
type racingStore struct {
	*objectstore.Memory
	rival Marker
	raced bool
}

func (s *racingStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	if !s.raced {
		s.raced = true
		Expect(writeJSON(ctx, s.Memory, MarkerKey, s.rival)).To(Succeed())
	}
	return s.Memory.PutIfAbsent(ctx, key, data)
}

var _ = Describe("Inspect", func() {
	ctx := context.Background()
	t0 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	It("reports an empty archive", func() {
		in, err := Inspect(ctx, &objectstore.Memory{})
		Expect(err).NotTo(HaveOccurred())
		Expect(in.HasDatabase).To(BeFalse())
		Expect(in.HasSnapshot).To(BeFalse())
		Expect(in.FirstRecoverabilityPoint).To(BeNil())
		Expect(in.LastBackup).To(BeNil())
		Expect(in.MissingKeys([]string{RoleNoise})).To(ConsistOf(RoleNoise))
	})

	It("derives recoverability from snapshots and the newest object", func() {
		store := &objectstore.Memory{}
		store.SetObject("db/0009/0000000000000001-0000000000000001.ltx", []byte("s1"), t0.Add(time.Hour))
		store.SetObject("db/0009/0000000000000001-0000000000000005.ltx", []byte("s2"), t0.Add(3*time.Hour))
		store.SetObject("db/0000/0000000000000006-0000000000000006.ltx", []byte("l0"), t0.Add(5*time.Hour))
		// Objects outside db/ are not database backups.
		store.SetObject("keys/noise.key", []byte("k"), t0.Add(9*time.Hour))

		in, err := Inspect(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(in.HasDatabase).To(BeTrue())
		Expect(in.HasSnapshot).To(BeTrue())
		Expect(*in.FirstRecoverabilityPoint).To(Equal(t0.Add(time.Hour)))
		Expect(*in.LastBackup).To(Equal(t0.Add(5 * time.Hour)))
	})

	It("records when the store received the heartbeat", func() {
		store := &objectstore.Memory{}
		store.SetObject(HeartbeatKey, []byte(`{"at":"2020-01-01T00:00:00Z"}`), t0)
		in, err := Inspect(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(in.Heartbeat.At.Year()).To(Equal(2020))
		Expect(*in.HeartbeatWritten).To(Equal(t0))
	})

	It("does not mistake a sibling prefix for the database", func() {
		store := &objectstore.Memory{}
		store.SetObject("db2/0009/x.ltx", []byte("s"), t0)
		in, err := Inspect(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(in.HasDatabase).To(BeFalse())
	})

	It("reports missing keys against the manifest", func() {
		store := &objectstore.Memory{}
		store.SetObject(KeyObject(RoleNoise), []byte("k"), t0)
		Expect(WriteManifest(ctx, store, &Manifest{Keys: map[string]KeyEntry{RoleNoise: {SHA256: "x", Size: 1}}})).To(Succeed())
		in, err := Inspect(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(in.MissingKeys([]string{RoleNoise, RoleDERP})).To(ConsistOf(RoleDERP))
	})

	It("reports a key as missing when only the manifest lists it", func() {
		store := &objectstore.Memory{}
		Expect(WriteManifest(ctx, store, &Manifest{Keys: map[string]KeyEntry{
			RoleNoise: {SHA256: "x", Size: 1},
			RoleDERP:  {SHA256: "y", Size: 5},
		}})).To(Succeed())
		// The DERP key was truncated or replaced: its size doesn't match.
		store.SetObject(KeyObject(RoleDERP), []byte("k"), t0)
		in, err := Inspect(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(in.MissingKeys([]string{RoleNoise, RoleDERP})).To(ConsistOf(RoleNoise, RoleDERP))
	})
})

var _ = Describe("KeyArchiver", func() {
	var (
		ctx   context.Context
		dir   string
		store *objectstore.Memory
		keys  []KeyFile
	)

	BeforeEach(func() {
		ctx = context.Background()
		dir = GinkgoT().TempDir()
		store = &objectstore.Memory{}
		keys = []KeyFile{
			{Role: RoleNoise, Path: filepath.Join(dir, "noise_private.key")},
			{Role: RoleDERP, Path: filepath.Join(dir, "derp_server_private.key")},
		}
	})

	newArchiver := func(keys []KeyFile) *KeyArchiver {
		return &KeyArchiver{Store: store, Keys: keys, Log: logr.Discard()}
	}

	It("waits until headscale has generated the keys", func() {
		done, err := newArchiver(keys).Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(done).To(BeFalse())
		Expect(store.Puts).To(BeEmpty())
	})

	It("archives keys as they appear and records them in the manifest", func() {
		a := newArchiver(keys)
		Expect(os.WriteFile(keys[0].Path, []byte("noise-key"), 0o600)).To(Succeed())

		done, err := a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(done).To(BeFalse(), "derp key is still missing")

		Expect(os.WriteFile(keys[1].Path, []byte("derp-key"), 0o600)).To(Succeed())
		done, err = a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(done).To(BeTrue())

		data, err := store.Get(ctx, KeyObject(RoleNoise))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("noise-key"))
		m, err := ReadManifest(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(m.Keys).To(HaveKeyWithValue(RoleNoise, HaveField("SHA256", sha256Hex([]byte("noise-key")))))
		Expect(m.Keys).To(HaveKeyWithValue(RoleDERP, HaveField("Size", int64(len("derp-key")))))
	})

	It("does not re-upload unchanged keys, even after a restart", func() {
		Expect(os.WriteFile(keys[0].Path, []byte("noise-key"), 0o600)).To(Succeed())
		only := keys[:1]
		a := newArchiver(only)
		for range 3 {
			_, err := a.Sync(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
		_, err := newArchiver(only).Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.Puts[KeyObject(RoleNoise)]).To(Equal(1))
		Expect(store.Puts[ManifestKey]).To(Equal(1))
	})

	It("re-uploads a key whose content changed", func() {
		only := keys[:1]
		a := newArchiver(only)
		Expect(os.WriteFile(keys[0].Path, []byte("v1"), 0o600)).To(Succeed())
		_, err := a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.WriteFile(keys[0].Path, []byte("v2"), 0o600)).To(Succeed())
		_, err = a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.Puts[KeyObject(RoleNoise)]).To(Equal(2))
		data, _ := store.Get(ctx, KeyObject(RoleNoise))
		Expect(string(data)).To(Equal("v2"))
	})

	It("re-uploads a key deleted from the archive", func() {
		Expect(os.WriteFile(keys[0].Path, []byte("noise-key"), 0o600)).To(Succeed())
		a := newArchiver(keys[:1])
		_, err := a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())

		store.Delete(KeyObject(RoleNoise))
		_, err = a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		data, err := store.Get(ctx, KeyObject(RoleNoise))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("noise-key"))
	})

	It("rewrites a manifest deleted from the archive", func() {
		Expect(os.WriteFile(keys[0].Path, []byte("noise-key"), 0o600)).To(Succeed())
		a := newArchiver(keys[:1])
		_, err := a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())

		store.Delete(ManifestKey)
		_, err = a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		m, err := ReadManifest(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(m).NotTo(BeNil())
		Expect(m.Keys).To(HaveKey(RoleNoise))
	})

	It("archives only the configured keys", func() {
		Expect(os.WriteFile(keys[0].Path, []byte("noise-key"), 0o600)).To(Succeed())
		Expect(os.WriteFile(keys[1].Path, []byte("derp-key"), 0o600)).To(Succeed())
		done, err := newArchiver(keys[:1]).Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(done).To(BeTrue())
		_, err = store.Get(ctx, KeyObject(RoleDERP))
		Expect(errors.Is(err, objectstore.ErrNotFound)).To(BeTrue())
	})

	It("recovers after an object store outage", func() {
		Expect(os.WriteFile(keys[0].Path, []byte("noise-key"), 0o600)).To(Succeed())
		a := newArchiver(keys[:1])
		store.Err = errors.New("unreachable")
		_, err := a.Sync(ctx)
		Expect(err).To(HaveOccurred())
		store.Err = nil
		done, err := a.Sync(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(done).To(BeTrue())
	})
})

var _ = Describe("Restorer", func() {
	var (
		ctx    context.Context
		dir    string
		dbPath string
		store  *objectstore.Memory
		keys   []KeyFile
	)

	archiveKey := func(role, content string) {
		Expect(store.Put(ctx, KeyObject(role), []byte(content))).To(Succeed())
		m, err := ReadManifest(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		if m == nil {
			m = &Manifest{Keys: map[string]KeyEntry{}}
		}
		m.Keys[role] = KeyEntry{SHA256: sha256Hex([]byte(content)), Size: int64(len(content))}
		Expect(WriteManifest(ctx, store, m)).To(Succeed())
	}

	stageDB := func() {
		Expect(os.WriteFile(dbPath+StagedSuffix, []byte("sqlite"), 0o644)).To(Succeed())
	}

	restorer := func() *Restorer {
		return &Restorer{Store: store, Keys: keys, DBPath: dbPath, Source: "s3://bucket/old", Log: logr.Discard()}
	}

	BeforeEach(func() {
		ctx = context.Background()
		dir = GinkgoT().TempDir()
		dbPath = filepath.Join(dir, "db.sqlite")
		store = &objectstore.Memory{}
		keys = []KeyFile{
			{Role: RoleNoise, Path: filepath.Join(dir, "noise_private.key")},
			{Role: RoleDERP, Path: filepath.Join(dir, "derp_server_private.key")},
		}
	})

	It("moves the database and verified keys into place", func() {
		archiveKey(RoleNoise, "noise-key")
		archiveKey(RoleDERP, "derp-key")
		stageDB()

		Expect(restorer().Restore(ctx)).To(Succeed())

		Expect(os.ReadFile(dbPath)).To(Equal([]byte("sqlite")))
		Expect(os.ReadFile(keys[0].Path)).To(Equal([]byte("noise-key")))
		Expect(os.ReadFile(keys[1].Path)).To(Equal([]byte("derp-key")))
		info, err := os.Stat(keys[0].Path)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Mode().Perm()).To(Equal(os.FileMode(0o600)))
		Expect(dbPath + StagedSuffix).NotTo(BeAnExistingFile())
		Expect(filepath.Join(dir, RestoredMarkerFile)).To(BeAnExistingFile())
	})

	It("creates the directory of a key configured in a subdirectory", func() {
		keys = []KeyFile{{Role: RoleNoise, Path: filepath.Join(dir, "keys", "noise_private.key")}}
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(os.ReadFile(keys[0].Path)).To(Equal([]byte("noise-key")))
	})

	It("moves SQLite sidecar files together with the database", func() {
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		Expect(os.WriteFile(dbPath+StagedSuffix+"-wal", []byte("wal"), 0o644)).To(Succeed())
		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(os.ReadFile(dbPath + "-wal")).To(Equal([]byte("wal")))
	})

	It("is idempotent once a recovery has completed", func() {
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(os.WriteFile(dbPath, []byte("written by headscale"), 0o644)).To(Succeed())

		By("re-running after litestream staged another copy")
		stageDB()
		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(dbPath + StagedSuffix).NotTo(BeAnExistingFile())
		Expect(os.ReadFile(dbPath)).To(Equal([]byte("written by headscale")))
	})

	It("completes a recovery interrupted after the database was moved into place", func() {
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		Expect(restorer().Restore(ctx)).To(Succeed())

		By("simulating a crash before the staged copies were cleaned up")
		stageDB()
		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(os.ReadFile(dbPath)).To(Equal([]byte("sqlite")))
	})

	It("completes a recovery interrupted before the database was moved into place", func() {
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		marker := filepath.Join(dir, RestoredMarkerFile)
		Expect(os.WriteFile(marker, []byte(`{}`), 0o600)).To(Succeed())
		Expect(os.WriteFile(keys[0].Path, []byte("noise-key"), 0o600)).To(Succeed())

		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(os.ReadFile(dbPath)).To(Equal([]byte("sqlite")))
		Expect(os.ReadFile(keys[0].Path)).To(Equal([]byte("noise-key")))
	})

	It("fails without the noise key and leaves no database behind", func() {
		archiveKey(RoleDERP, "derp-key")
		stageDB()
		err := restorer().Restore(ctx)
		Expect(errors.Is(err, ErrKeysMissing)).To(BeTrue())
		Expect(dbPath).NotTo(BeAnExistingFile())
	})

	It("fails when the archive has no manifest", func() {
		stageDB()
		Expect(errors.Is(restorer().Restore(ctx), ErrKeysMissing)).To(BeTrue())
	})

	It("tolerates a missing DERP key", func() {
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(keys[1].Path).NotTo(BeAnExistingFile())
	})

	It("rejects a key whose checksum does not match the manifest", func() {
		archiveKey(RoleNoise, "noise-key")
		Expect(store.Put(ctx, KeyObject(RoleNoise), []byte("tampered"))).To(Succeed())
		stageDB()
		Expect(restorer().Restore(ctx)).To(MatchError(ContainSubstring("checksum mismatch")))
		Expect(dbPath).NotTo(BeAnExistingFile())
		Expect(keys[0].Path).NotTo(BeAnExistingFile())
	})

	It("never overwrites an existing database", func() {
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		Expect(os.WriteFile(dbPath, []byte("live data"), 0o644)).To(Succeed())
		Expect(errors.Is(restorer().Restore(ctx), ErrDatabaseExists)).To(BeTrue())
		Expect(os.ReadFile(dbPath)).To(Equal([]byte("live data")))
	})

	It("fails when litestream did not restore the database", func() {
		archiveKey(RoleNoise, "noise-key")
		Expect(restorer().Restore(ctx)).To(MatchError(ContainSubstring("litestream restore did not complete")))
	})

	It("can be retried after a failed download", func() {
		archiveKey(RoleNoise, "noise-key")
		stageDB()
		store.Err = errors.New("unreachable")
		Expect(restorer().Restore(ctx)).To(HaveOccurred())
		Expect(dbPath).NotTo(BeAnExistingFile())
		store.Err = nil
		Expect(restorer().Restore(ctx)).To(Succeed())
		Expect(os.ReadFile(keys[0].Path)).To(Equal([]byte("noise-key")))
	})
})

var _ = Describe("ParseKeyFile", func() {
	It("parses role=path", func() {
		k, err := ParseKeyFile("noise=/var/lib/headscale/noise_private.key")
		Expect(err).NotTo(HaveOccurred())
		Expect(k).To(Equal(KeyFile{Role: RoleNoise, Path: "/var/lib/headscale/noise_private.key"}))
	})

	DescribeTable("rejects malformed values",
		func(s string) {
			_, err := ParseKeyFile(s)
			Expect(err).To(HaveOccurred())
		},
		Entry("no separator", "noise"),
		Entry("empty role", "=/path"),
		Entry("empty path", "noise="),
	)
})

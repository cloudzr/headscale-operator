package archive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/infradohq/headscale-operator/internal/objectstore"
)

var _ = Describe("ArchivedTXID", func() {
	It("returns the highest transaction of any level", func() {
		Expect(ArchivedTXID([]objectstore.Object{
			{Key: "db/0009/0000000000000001-0000000000000005.ltx"},
			{Key: "db/0000/0000000000000006-000000000000000a.ltx"},
			{Key: "db/0001/0000000000000001-0000000000000008.ltx"},
			{Key: "db/0000/not-an-ltx-file"},
			{Key: "db/0000/zzzz-zzzz.ltx"},
		})).To(Equal(uint64(10)))
	})

	It("is 0 for an empty archive", func() {
		Expect(ArchivedTXID(nil)).To(BeZero())
	})
})

var _ = Describe("parseTXID", func() {
	const db = "/var/lib/headscale/db.sqlite"

	DescribeTable("reads litestream_txid",
		func(body string, want uint64) {
			got, err := parseTXID(strings.NewReader(body), db)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(want))
		},
		Entry("labelled series", `# HELP litestream_txid The current transaction ID
# TYPE litestream_txid gauge
litestream_txid{db="/var/lib/headscale/db.sqlite"} 42
litestream_txid_other 7
`, uint64(42)),
		Entry("exponent notation", `litestream_txid{db="/var/lib/headscale/db.sqlite"} 1.234567e+06`, uint64(1234567)),
		Entry("the matching database among several", `litestream_txid{db="/other.sqlite"} 1
litestream_txid{db="/var/lib/headscale/db.sqlite"} 9`, uint64(9)),
		Entry("a single series with another label", `litestream_txid{db="db.sqlite"} 3`, uint64(3)),
		Entry("no series yet", `litestream_db_size{db="/var/lib/headscale/db.sqlite"} 0`, uint64(0)),
	)

	It("rejects a malformed value", func() {
		_, err := parseTXID(strings.NewReader(`litestream_txid{db="x"} abc`), db)
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ReplicationReporter", func() {
	var (
		ctx      context.Context
		store    *objectstore.Memory
		local    string
		status   int
		server   *httptest.Server
		reporter *ReplicationReporter
		now      time.Time
	)

	BeforeEach(func() {
		ctx = context.Background()
		store = &objectstore.Memory{}
		local, status = "0", http.StatusOK
		server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`litestream_txid{db="/data/db.sqlite"} ` + local + "\n"))
		}))
		DeferCleanup(server.Close)
		now = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
		reporter = &ReplicationReporter{Store: store, MetricsURL: server.URL, DBPath: "/data/db.sqlite", Now: func() time.Time { return now }}
	})

	report := func() Heartbeat {
		hb, err := reporter.Report(ctx)
		Expect(err).NotTo(HaveOccurred())
		stored, err := ReadHeartbeat(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		Expect(stored.At).To(BeTemporally("==", hb.At))
		return hb
	}

	It("is not lagging while the archive has every transaction", func() {
		store.SetObject("db/0000/0000000000000001-0000000000000005.ltx", nil, now)
		local = "5"
		hb := report()
		Expect(hb.LocalTXID).To(Equal(uint64(5)))
		Expect(hb.ArchivedTXID).To(Equal(uint64(5)))
		Expect(hb.LaggingSince).To(BeNil())
	})

	It("is not lagging before the database exists", func() {
		Expect(report().LaggingSince).To(BeNil())
	})

	It("tracks since when the archive is behind, until it catches up", func() {
		store.SetObject("db/0000/0000000000000001-0000000000000005.ltx", nil, now)
		local = "7"
		start := now
		Expect(*report().LaggingSince).To(BeTemporally("==", start))

		now = now.Add(time.Minute)
		local = "9"
		Expect(*report().LaggingSince).To(BeTemporally("==", start), "still behind since the first observation")

		store.SetObject("db/0000/0000000000000006-0000000000000009.ltx", nil, now)
		now = now.Add(time.Second)
		Expect(report().LaggingSince).To(BeNil())
	})

	It("is not lagging while a busy database's archive keeps advancing", func() {
		start := now
		for i := uint64(1); i <= 10; i++ {
			// Every sample finds the archive one transaction behind, but past
			// what was local at the previous sample.
			archived := i * 100
			store.SetObject(fmt.Sprintf("db/0000/%016x-%016x.ltx", archived-99, archived), nil, now)
			local = strconv.FormatUint(archived+1, 10)
			hb := report()
			Expect(hb.LaggingSince).NotTo(BeNil())
			Expect(*hb.LaggingSince).To(BeTemporally("==", now), "a new lag window starts at each sample")
			now = now.Add(30 * time.Second)
		}
		Expect(now.Sub(start)).To(BeNumerically(">", 2*time.Minute))
	})

	It("keeps the lag window while the archive doesn't reach the position it started at", func() {
		store.SetObject("db/0000/0000000000000001-0000000000000064.ltx", nil, now)
		local = "200"
		start := now
		report()

		By("advancing, but not as far as transaction 200")
		now = now.Add(time.Minute)
		store.SetObject("db/0000/0000000000000065-0000000000000096.ltx", nil, now)
		local = "300"
		Expect(*report().LaggingSince).To(BeTemporally("==", start))
	})

	It("reports unreadable Litestream metrics without failing", func() {
		status = http.StatusInternalServerError
		hb := report()
		Expect(hb.Error).To(ContainSubstring("500"))
	})

	It("fails when the archive can't be written", func() {
		store.Err = errors.New("access denied")
		_, err := reporter.Report(ctx)
		Expect(err).To(MatchError(ContainSubstring("access denied")))
	})
})

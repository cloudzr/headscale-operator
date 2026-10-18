package objectstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeTable("ParseDestination",
	func(path, bucket, prefix string, wantErr bool) {
		b, p, err := ParseDestination(path)
		if wantErr {
			Expect(err).To(HaveOccurred())
			return
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(b).To(Equal(bucket))
		Expect(p).To(Equal(prefix))
	},
	Entry("bucket only", "s3://bucket", "bucket", "", false),
	Entry("bucket and prefix", "s3://bucket/a/b", "bucket", "a/b", false),
	Entry("trailing slash", "s3://bucket/a/", "bucket", "a", false),
	Entry("wrong scheme", "gs://bucket/a", "", "", true),
	Entry("no bucket", "s3:///a", "", "", true),
)

var _ = DescribeTable("Join",
	func(parts []string, want string) { Expect(Join(parts...)).To(Equal(want)) },
	Entry("empty root", []string{"", "keys/noise.key"}, "keys/noise.key"),
	Entry("nested root", []string{"a/b", "keys/noise.key"}, "a/b/keys/noise.key"),
	Entry("extra slashes", []string{"/a/", "/db/"}, "a/db"),
)

var _ = Describe("Memory", func() {
	ctx := context.Background()

	It("round-trips objects and reports missing keys", func() {
		m := &Memory{}
		Expect(m.Put(ctx, "a", []byte("1"))).To(Succeed())
		Expect(m.Get(ctx, "a")).To(Equal([]byte("1")))
		_, err := m.Get(ctx, "b")
		Expect(errors.Is(err, ErrNotFound)).To(BeTrue())
	})

	It("lists by prefix in key order", func() {
		m := &Memory{}
		for _, k := range []string{"db/2", "db/1", "db2/1", "keys/x"} {
			Expect(m.Put(ctx, k, []byte(k))).To(Succeed())
		}
		objs, err := m.List(ctx, "db/")
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(2))
		Expect(objs[0].Key).To(Equal("db/1"))
		Expect(objs[1].Key).To(Equal("db/2"))
	})
})

var _ = Describe("Memory PutIfAbsent", func() {
	ctx := context.Background()

	It("writes once and then reports the existing object", func() {
		m := &Memory{}
		Expect(m.PutIfAbsent(ctx, "marker", []byte("a"))).To(Succeed())
		err := m.PutIfAbsent(ctx, "marker", []byte("b"))
		Expect(errors.Is(err, ErrExists)).To(BeTrue())
		Expect(m.Get(ctx, "marker")).To(Equal([]byte("a")))
	})
})

// The S3 implementation is exercised against a real S3-compatible server when
// OBJECTSTORE_TEST_ENDPOINT is set (e.g. a local versitygw or MinIO), using
// OBJECTSTORE_TEST_BUCKET and AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
var _ = Describe("S3", func() {
	ctx := context.Background()

	BeforeEach(func() {
		if os.Getenv("OBJECTSTORE_TEST_ENDPOINT") == "" {
			Skip("OBJECTSTORE_TEST_ENDPOINT not set")
		}
	})

	newStore := func(prefix string) Store {
		s, err := NewS3(Config{
			DestinationPath: "s3://" + os.Getenv("OBJECTSTORE_TEST_BUCKET") + prefix,
			Endpoint:        os.Getenv("OBJECTSTORE_TEST_ENDPOINT"),
			Region:          "us-east-1",
			AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		})
		Expect(err).NotTo(HaveOccurred())
		return s
	}

	for _, root := range []string{"", "/it/" + fmt.Sprint(time.Now().UnixNano())} {
		It(fmt.Sprintf("puts, gets and lists relative to root %q", root), func() {
			s := newStore(root)
			Expect(s.Put(ctx, "db/0009/a.ltx", []byte("a"))).To(Succeed())
			Expect(s.Put(ctx, "db2/x", []byte("x"))).To(Succeed())
			Expect(s.Put(ctx, "keys/noise.key", []byte("k"))).To(Succeed())

			Expect(s.Get(ctx, "keys/noise.key")).To(Equal([]byte("k")))
			_, err := s.Get(ctx, "keys/missing.key")
			Expect(errors.Is(err, ErrNotFound)).To(BeTrue())

			objs, err := s.List(ctx, "db/")
			Expect(err).NotTo(HaveOccurred())
			Expect(objs).To(HaveLen(1))
			Expect(objs[0].Key).To(Equal("db/0009/a.ltx"))
			Expect(objs[0].LastModified).NotTo(BeZero())

			// Stores without conditional writes overwrite instead of failing.
			key := fmt.Sprintf("cond/%d", time.Now().UnixNano())
			Expect(s.PutIfAbsent(ctx, key, []byte("first"))).To(Succeed())
			if err := s.PutIfAbsent(ctx, key, []byte("second")); err != nil {
				Expect(errors.Is(err, ErrExists)).To(BeTrue(), err.Error())
				Expect(s.Get(ctx, key)).To(Equal([]byte("first")))
			}
		})
	}
})

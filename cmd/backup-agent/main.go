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

// Command backup-agent archives headscale's private keys next to the
// Litestream database replica, and restores them during a recovery.
//
//	backup-agent archive-keys --destination s3://bucket/prefix --key noise=/var/lib/headscale/noise_private.key
//	backup-agent restore      --destination s3://bucket/prefix --key noise=... --db /var/lib/headscale/db.sqlite
//
// Credentials are read from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, the
// same variables Litestream uses. When they are unset the ambient AWS
// credential chain (IRSA, pod identity) is used.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/infradohq/headscale-operator/internal/archive"
	"github.com/infradohq/headscale-operator/internal/objectstore"
)

// keyFlags collects repeated --key role=path flags.
type keyFlags []archive.KeyFile

func (k *keyFlags) String() string {
	parts := make([]string, 0, len(*k))
	for _, key := range *k {
		parts = append(parts, key.Role+"="+key.Path)
	}
	return strings.Join(parts, ",")
}

func (k *keyFlags) Set(s string) error {
	key, err := archive.ParseKeyFile(s)
	if err != nil {
		return err
	}
	*k = append(*k, key)
	return nil
}

type storeFlags struct {
	destination string
	endpoint    string
	region      string
}

func (s *storeFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&s.destination, "destination", "", "Archive location, s3://<bucket>[/<prefix>] (required)")
	fs.StringVar(&s.endpoint, "endpoint", "", "S3-compatible endpoint URL (empty for AWS S3)")
	fs.StringVar(&s.region, "region", "", "Bucket region")
}

func (s *storeFlags) open() (objectstore.Store, error) {
	if s.destination == "" {
		return nil, fmt.Errorf("--destination is required")
	}
	return objectstore.NewS3(objectstore.Config{
		DestinationPath: s.destination,
		Endpoint:        s.endpoint,
		Region:          s.region,
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	})
}

func main() {
	zapConfig := zap.NewProductionConfig()
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapLog, err := zapConfig.Build()
	if err != nil {
		panic(err)
	}
	defer func() { _ = zapLog.Sync() }()
	log := zapr.NewLogger(zapLog)

	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: backup-agent <archive-keys|restore> [flags]")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(logr.NewContext(context.Background(), log), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "archive-keys":
		err = archiveKeys(ctx, os.Args[2:])
	case "restore":
		err = restore(ctx, os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Error(err, "Backup agent failed")
		// Flush before exiting: os.Exit skips deferred calls.
		_ = zapLog.Sync()
		os.Exit(1)
	}
}

func archiveKeys(ctx context.Context, args []string) error {
	log := logr.FromContextOrDiscard(ctx)
	fs := flag.NewFlagSet("archive-keys", flag.ExitOnError)
	var store storeFlags
	var keys keyFlags
	store.register(fs)
	fs.Var(&keys, "key", "Private key to archive as role=path (repeatable)")
	interval := fs.Duration("interval", 30*time.Second, "How often to check the keys for changes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("at least one --key is required")
	}
	s, err := store.open()
	if err != nil {
		return err
	}

	log.Info("Starting private key archiving",
		"destination", store.destination, "keys", keys.String(), "interval", interval.String())
	(&archive.KeyArchiver{Store: s, Keys: keys, Log: log}).Run(ctx, *interval)
	log.Info("Stopped private key archiving")
	return nil
}

func restore(ctx context.Context, args []string) error {
	log := logr.FromContextOrDiscard(ctx)
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	var store storeFlags
	var keys keyFlags
	store.register(fs)
	fs.Var(&keys, "key", "Private key to restore as role=path (repeatable)")
	dbPath := fs.String("db", "/var/lib/headscale/db.sqlite", "Path of the headscale SQLite database")
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := store.open()
	if err != nil {
		return err
	}

	log.Info("Starting recovery", "source", store.destination, "db", *dbPath, "keys", keys.String())
	r := &archive.Restorer{Store: s, Keys: keys, DBPath: *dbPath, Source: store.destination, Log: log}
	return r.Restore(ctx)
}

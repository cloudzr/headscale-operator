# Headscale Backup Agent

The backup agent archives Headscale's private keys next to the Litestream replica of its database, and restores them during a recovery. The operator runs it automatically for Headscale resources that set `spec.backup` or `spec.bootstrap.recovery`; it is not meant to be deployed by hand.

## Overview

Litestream only replicates SQLite databases, but a Headscale server is identified by its **noise private key**: a database restored without it forces every client to re-register. The agent closes that gap so that one archive is enough to recover a server with its identity.

```
┌──────────────────────── Headscale Pod ────────────────────────┐
│                                                               │
│  ┌────────────┐   ┌──────────────┐   ┌──────────────────────┐ │
│  │ litestream │   │  headscale   │   │ backup-agent         │ │
│  │ (sidecar)  │   │              │   │ archive-keys         │ │
│  └─────┬──────┘   └──────┬───────┘   └──────────┬───────────┘ │
│        │ db.sqlite + WAL │ keys                 │ keys (ro)   │
│        └─────────── /var/lib/headscale (PVC) ───┘             │
└────────┼────────────────────────────────────────┼─────────────┘
         ▼                                        ▼
   s3://bucket/prefix/db/              s3://bucket/prefix/keys/
```

## Commands

### `archive-keys` (sidecar)

```sh
backup-agent archive-keys \
  --destination s3://bucket/prefix \
  --endpoint https://minio.example.com --region us-east-1 \
  --key noise=/var/lib/headscale/noise_private.key \
  --key derp=/var/lib/headscale/derp_server_private.key \
  --interval 30s
```

- Waits until Headscale has generated each key (it does so on first start).
- Uploads `keys/<role>.key`, then records its SHA-256 in `keys/manifest.json`.
- Re-uploads a key only when its checksum differs from the manifest, also across restarts.
- Retries forever on object store errors: an outage never stops the agent or Headscale.

### `restore` (recovery Job)

```sh
backup-agent restore \
  --destination s3://bucket/prefix \
  --key noise=/var/lib/headscale/noise_private.key \
  --db /var/lib/headscale/db.sqlite
```

Runs after `litestream restore -o <db>.restoring` and:

1. refuses to run if a database already exists (unless it completed this recovery before, which makes re-runs a no-op);
2. downloads every key in the manifest and verifies its checksum (the noise key is required, the DERP key optional);
3. moves the keys into place, then the database last, so an interrupted recovery never leaves an initialized-looking volume;
4. writes `.headscale-operator-restored` next to the database.

## Credentials

The agent reads `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, the same variables as Litestream. Without them it uses the ambient AWS credential chain (IRSA, EKS Pod Identity, instance metadata).

## Building

```sh
make build-backup-agent
make docker-build-backup-agent BACKUP_AGENT_IMG=<registry>/headscale-operator/backup-agent:<tag>
```

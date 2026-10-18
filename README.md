# Headscale Operator

A Kubernetes operator for managing [Headscale](https://github.com/juanfont/headscale) - an open source, self-hosted implementation of the Tailscale control server.

## Overview

The Headscale Operator simplifies the deployment and management of Headscale instances on Kubernetes. It provides a declarative, GitOps-friendly way to configure and deploy Headscale with all its configuration options through Kubernetes Custom Resources.

## Features

- **Declarative Configuration**: Define your entire Headscale setup as a Kubernetes Custom Resource
- **Automatic Deployment**: Manages StatefulSets, Services, ConfigMaps, and PersistentVolumes
- **API Key Management**: Automatic API key creation and rotation with configurable expiration
- **ACL Policy Management**: Declarative auto-approval of subnet routes and exit nodes via the `HeadscaleAutoApprover` CRD
- **Full Config Support**: `spec.config` is a free-form passthrough to Headscale's `config.yaml` — every key Headscale supports works (database, DERP, DNS/MagicDNS, OIDC, TLS/Let's Encrypt, policy, …), and new/renamed keys in future Headscale releases need no operator change. Headscale validates the config itself at startup; the result is reflected on the `Headscale` status.
- **Backup and Recovery**: Continuous archiving of the SQLite database and of Headscale's private keys to S3-compatible storage with Litestream, and CNPG-style bootstrap of new instances from an archive (latest or point-in-time)
- **Observability**: Built-in metrics endpoint for monitoring
- **Production Ready**: Supports high availability with persistent storage

## Table of Contents

- [Headscale Operator](#headscale-operator)
  - [Overview](#overview)
  - [Features](#features)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
      - [Using Helm (Recommended)](#using-helm-recommended)
    - [Quick Start](#quick-start)
  - [Usage](#usage)
    - [Managing Users](#managing-users)
      - [Creating a User](#creating-a-user)
      - [User Properties](#user-properties)
      - [Viewing Users](#viewing-users)
    - [Managing PreAuth Keys](#managing-preauth-keys)
      - [Creating a PreAuth Key](#creating-a-preauth-key)
      - [PreAuth Key Properties](#preauth-key-properties)
      - [Retrieving PreAuth Keys](#retrieving-preauth-keys)
      - [Using PreAuth Keys](#using-preauth-keys)
      - [PreAuth Key Examples](#preauth-key-examples)
    - [Managing Auto-Approve Routes](#managing-auto-approve-routes)
      - [Prerequisites](#prerequisites-1)
      - [Creating an Auto-Approver](#creating-an-auto-approver)
      - [Auto-Approver Properties](#auto-approver-properties)
      - [Viewing Auto-Approvers](#viewing-auto-approvers)
      - [Combining With PreAuth Keys](#combining-with-preauth-keys)
      - [Auto-Approver Examples](#auto-approver-examples)
    - [API Key Management](#api-key-management)
    - [Backup and Recovery](#backup-and-recovery)
      - [How It Works](#how-it-works)
      - [Enabling Continuous Backup](#enabling-continuous-backup)
      - [Backup Status](#backup-status)
      - [Recovering an Instance](#recovering-an-instance)
      - [Safety Guarantees](#safety-guarantees)
      - [Securing the Archive](#securing-the-archive)
      - [Monitoring Backups](#monitoring-backups)
      - [Disaster Recovery Runbook](#disaster-recovery-runbook)
      - [Limitations](#limitations)
    - [Uninstallation](#uninstallation)
  - [Development](#development)
    - [Building from Source](#building-from-source)
    - [Running Locally](#running-locally)
    - [Deploying to Cluster](#deploying-to-cluster)
    - [Running Tests](#running-tests)
  - [Contributing](#contributing)
  - [Acknowledgments](#acknowledgments)

## Getting Started

### Prerequisites

- Go 1.25.0+
- Docker 17.03+
- kubectl 1.11.3+
- Access to a Kubernetes 1.11.3+ cluster

### Installation

#### Using Helm (Recommended)

```sh
helm install headscale-operator oci://ghcr.io/infradohq/headscale-operator/charts/headscale-operator:$LATEST_VERSION
```

### Quick Start

1. Create a namespace for Headscale:

```sh
kubectl create namespace headscale
```

1. Create a Headscale instance:

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: Headscale
metadata:
  name: headscale-sample
  namespace: headscale
spec:
  version: "v0.29.4"
  replicas: 1
  config:
    server_url: http://vpn.headscale.local
    grpc_allow_insecure: true
    # Persistent paths must live under /var/lib/headscale (the data volume mount).
    noise:
      private_key_path: /var/lib/headscale/noise_private.key
    prefixes:
      v4: 100.64.0.0/10
      v6: fd7a:115c:a1e0::/48
    derp:
      server:
        enabled: false
      urls:
        - https://controlplane.tailscale.com/derpmap/default
    disable_check_updates: false
    database:
      type: sqlite
      sqlite:
        path: /var/lib/headscale/db.sqlite
    dns:
      magic_dns: false
      override_local_dns: false
  # Automatic API key management (optional)
  api_key:
    auto_manage: true  # Automatically create and rotate API keys
    expiration: "2160h"      # API key expires in 90 days (2160 hours)
    rotation_buffer: "240h"   # Rotate 10 days (240 hours) before expiration
```

> **About `spec.config`:** this maps 1:1 to Headscale's `config.yaml` and is passed
> through verbatim — the operator does not validate individual keys, so any option
> supported by the Headscale version you deploy is accepted (and removed/renamed
> keys won't break the CRD). Headscale validates the config when it starts; a bad
> config surfaces as a crashing pod whose error is reported on the `Headscale`
> resource's `Ready` condition (and as an event). The example above is a complete,
> minimal config — you own it, so include everything Headscale requires (e.g.
> `noise.private_key_path`, at least one `prefixes` range, a `database.sqlite.path`
> when using SQLite, a DERP map via `derp.urls` or an embedded `derp.server`, and a
> `dns` block — `dns.override_local_dns` defaults to `true`, which then requires
> `dns.nameservers.global`). Put persistent paths under
> `/var/lib/headscale` (the data volume mount). The only keys the operator fills in
> for you are the ones it has to wire to Kubernetes itself, when omitted:
> `listen_addr` (`0.0.0.0:8080`), `metrics_listen_addr` (`0.0.0.0:9090`),
> `grpc_listen_addr` (`0.0.0.0:50443`), and `unix_socket`
> (`/var/run/headscale/headscale.sock`); it also requires `server_url`
> (`ConfigValid` condition).

Apply the configuration:

```sh
kubectl apply -f config/samples/headscale_v1beta1_headscale.yaml
```

Or use kustomize:

```sh
kubectl apply -k config/samples/
```

## Usage

The operator will automatically create and manage the following resources:

- A StatefulSet running Headscale
- A ConfigMap with the Headscale configuration
- Services for HTTP, gRPC, and metrics endpoints
- PersistentVolumeClaims for data storage
- API key management sidecar (if enabled)
- Kubernetes Secret with the API key (if auto-managed)

### Managing Users

The operator provides the `HeadscaleUser` custom resource to manage users in your Headscale instance.

#### Creating a User

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscaleUser
metadata:
  name: alice
  namespace: headscale
spec:
  # Reference to the Headscale instance
  headscaleRef: headscale-sample
  
  # Username (immutable after creation)
  username: alice
  
  # Optional: Display name for the user
  displayName: Alice Smith
  
  # Optional: Email address
  email: alice@example.com
  
  # Optional: Profile picture URL
  pictureURL: https://example.com/alice.jpg
```

Apply the user:

```sh
kubectl apply -f headscaleuser.yaml
```

#### User Properties

- **username**: Must be unique and follow DNS label rules (lowercase alphanumeric with hyphens). This field is **immutable** after creation.
- **displayName**: Human-readable name (max 255 characters). Immutable after creation.
- **email**: Valid email address (max 320 characters). Immutable after creation.
- **pictureURL**: HTTP(S) URL to profile picture (max 2048 characters). Immutable after creation.

#### Viewing Users

```sh
# List all users
kubectl get headscaleuser -n headscale

# Get user details
kubectl get headscaleuser alice -n headscale -o yaml

# View user status including Headscale UserID
kubectl get headscaleuser alice -n headscale -o jsonpath='{.status.userId}'
```

### Managing PreAuth Keys

The operator provides the `HeadscalePreAuthKey` custom resource to manage pre-authentication keys for registering nodes.

#### Creating a PreAuth Key

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscalePreAuthKey
metadata:
  name: dev-key
  namespace: headscale
spec:
  # Reference to the Headscale instance
  headscaleRef: headscale-sample
  
  # Reference to a HeadscaleUser resource
  headscaleUserRef: alice
  
  # Alternatively, specify user ID directly:
  # userId: 1
  
  # Key expires after 24 hours
  expiration: "24h"
  
  # Can be used only once (set to true for multiple uses)
  reusable: false
  
  # Creates ephemeral nodes (automatically removed when disconnected)
  ephemeral: false
  
  # Automatically assign tags to nodes using this key
  tags:
    - "tag:dev"
    - "tag:laptop"
  
  # Optional: Secret name (defaults to resource name)
  secretName: alice-dev-key
```

Apply the preauth key:

```sh
kubectl apply -f headscalepreauthkey.yaml
```

#### PreAuth Key Properties

- **headscaleRef**: Name of the Headscale instance (required)
- **headscaleUserRef**: Name of the HeadscaleUser resource (use this OR userId)
- **userId**: Numeric user ID from Headscale (use this OR headscaleUserRef)
- **expiration**: Duration string (e.g., "30m", "24h", "1h30m"). Default: "1h"
- **reusable**: Whether key can be used multiple times. Default: false
- **ephemeral**: Whether nodes should be ephemeral. Default: false
- **tags**: List of tags to assign (format: "tag:name")
- **secretName**: Name of the Kubernetes secret to store the key. Defaults to the resource name.

#### Retrieving PreAuth Keys

The generated preauth key is stored in a Kubernetes Secret:

```sh
# Get the preauth key
kubectl get secret alice-dev-key -n headscale -o jsonpath='{.data.key}' | base64 -d

# View all preauth keys
kubectl get headscalepreauthkey -n headscale

# View details
kubectl get headscalepreauthkey dev-key -n headscale -o yaml
```

#### Using PreAuth Keys

Use the retrieved key to register a new node to your Headscale network:

```sh
# Get the key
KEY=$(kubectl get secret alice-dev-key -n headscale -o jsonpath='{.data.key}' | base64 -d)

# Register a node using Tailscale client
tailscale up --login-server=https://headscale.example.com --authkey=$KEY
```

#### PreAuth Key Examples

**One-time use key for a single device:**

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscalePreAuthKey
metadata:
  name: laptop-key
spec:
  headscaleRef: headscale-sample
  headscaleUserRef: alice
  expiration: "1h"
  reusable: false
```

**Reusable key for multiple CI/CD runners:**

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscalePreAuthKey
metadata:
  name: ci-runner-key
spec:
  headscaleRef: headscale-sample
  headscaleUserRef: ci-user
  expiration: "720h"  # 30 days
  reusable: true
  ephemeral: true     # Auto-cleanup disconnected runners
  tags:
    - "tag:ci"
    - "tag:ephemeral"
```

**Key for temporary test environments:**

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscalePreAuthKey
metadata:
  name: test-env-key
spec:
  headscaleRef: headscale-sample
  userId: 5
  expiration: "2h"
  ephemeral: true
  tags:
    - "tag:test"
```

### Managing Auto-Approve Routes

The operator provides the `HeadscaleAutoApprover` custom resource to declaratively manage [auto-approved subnet routes and exit nodes](https://headscale.net/stable/ref/routes/#automatically-approve-routes-of-a-subnet-router) in your Headscale instance. Each `HeadscaleAutoApprover` contributes entries to the parent Headscale's policy document; the operator merges all auto-approvers targeting a Headscale and pushes the result via the gRPC `SetPolicy` API.

#### Prerequisites

Auto-approval lives in the Headscale policy document, which the operator can only write when Headscale is configured with `policy.mode: database`. The parent `Headscale` resource must also declare the tag owners that the auto-approver references. Both fields are set on the `Headscale` spec:

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: Headscale
metadata:
  name: headscale-sample
  namespace: headscale
spec:
  # ...
  config:
    policy:
      mode: database          # required for SetPolicy
  acl_policy:
    tag_owners:
      "tag:router": ["admin@example.com"]
      "tag:exit":   ["admin@example.com"]
    inline: |                 # optional base policy for acls/groups/hosts/ssh
      {
        "acls": [{"action": "accept", "src": ["*"], "dst": ["*:*"]}]
      }
```

`acl_policy.inline` accepts JSON or HuJSON (comments and trailing commas are fine). The operator parses it, merges in `tag_owners` and any `HeadscaleAutoApprover` entries, then pushes the result via `SetPolicy`. The CR is the source of truth — `headscale policy get` shows the rendered output as strict JSON.

#### Creating an Auto-Approver

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscaleAutoApprover
metadata:
  name: k8s-network
  namespace: headscale
spec:
  # Reference to the Headscale instance (same namespace)
  headscaleRef: headscale-sample

  # Routes a node carrying any of the listed tags will have
  # auto-approved when announced via --advertise-routes.
  routes:
    - cidr: 10.10.0.0/16
      tags: ["tag:router"]

  # Tags whose nodes will be auto-approved as exit nodes.
  exitNodeTags: ["tag:exit"]
```

Apply:

```sh
kubectl apply -f headscaleautoapprover.yaml
```

#### Auto-Approver Properties

- **headscaleRef**: Name of the Headscale instance in the same namespace. Required.
- **routes**: List of `{cidr, tags}` pairs. A node registered with one of the listed tags has the matching CIDR auto-approved when it advertises that route. Each tag must be declared in the parent's `acl_policy.tag_owners`.
- **exitNodeTags**: List of tags whose nodes are auto-approved as exit nodes when they advertise themselves as such.

At least one of `routes` or `exitNodeTags` must be specified. Multiple `HeadscaleAutoApprover` resources may target the same Headscale; the operator merges them deterministically into a single `autoApprovers` block.

#### Viewing Auto-Approvers

```sh
# List all auto-approvers in a namespace
kubectl get headscaleautoapprover -n headscale

# Inspect the Ready condition (status=True / reason=PolicyApplied means push succeeded)
kubectl get headscaleautoapprover k8s-network -n headscale \
  -o jsonpath='{.status.conditions[?(@.type=="Ready")]}'

# Inspect the live merged policy stored in Headscale
kubectl exec -n headscale -c headscale <headscale-pod> -- headscale policy get
```

The `Ready` condition reasons are:

- `PolicyApplied` — the merged policy was successfully pushed via gRPC.
- `HeadscaleNotFound` — the referenced Headscale doesn't exist in the same namespace.
- `PolicyModeUnsupported` — the parent Headscale isn't configured with `policy.mode: database`.
- `PolicyPushFailed` — the gRPC `SetPolicy` call returned an error (see `.status.conditions[].message` for details).

#### Combining With PreAuth Keys

An auto-approver only takes effect when a node is actually carrying the matching tag. The most common pattern is to issue a tagged preauth key for the subnet router:

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscalePreAuthKey
metadata:
  name: subnet-router-key
  namespace: headscale
spec:
  headscaleRef: headscale-sample
  headscaleUserRef: alice
  reusable: true
  tags: ["tag:router"]
```

A node registered with this key, advertising e.g. `--advertise-routes=10.10.0.0/16`, will have its routes auto-approved.

#### Auto-Approver Examples

**Auto-approve a Kubernetes pod CIDR via a subnet router:**

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscaleAutoApprover
metadata:
  name: pod-network
spec:
  headscaleRef: headscale-sample
  routes:
    - cidr: 10.244.0.0/16
      tags: ["tag:router"]
```

**Auto-approve a tagged exit node:**

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscaleAutoApprover
metadata:
  name: home-exit-node
spec:
  headscaleRef: headscale-sample
  exitNodeTags: ["tag:exit"]
```

**Per-team segmentation — each team owns its own CIDR:**

```yaml
---
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscaleAutoApprover
metadata:
  name: team-prod-routes
  namespace: headscale
spec:
  headscaleRef: headscale-sample
  routes:
    - cidr: 10.10.0.0/16
      tags: ["tag:router"]
---
apiVersion: headscale.infrado.cloud/v1beta1
kind: HeadscaleAutoApprover
metadata:
  name: team-staging-routes
  namespace: headscale
spec:
  headscaleRef: headscale-sample
  routes:
    - cidr: 10.20.0.0/16
      tags: ["tag:router"]
```

The operator merges both into a single `autoApprovers.routes` map and pushes one consolidated policy.

### API Key Management

When API key auto-management is enabled, the sidecar creates a Kubernetes Secret containing the API key:

```sh
# Get the API key
kubectl get secret headscale-api-key -n headscale -o jsonpath='{.data.api-key}' | base64 -d

# View full secret details
kubectl get secret headscale-api-key -n headscale -o yaml
```

The secret contains:

- `api-key`: The actual API key for authenticating with Headscale
- `expiration`: When the API key will expire (RFC3339 format)
- `created-at`: When the API key was created (RFC3339 format)

For more details on API key management, see [cmd/apikey-manager/README.md](cmd/apikey-manager/README.md).

### Backup and Recovery

The operator can continuously archive a Headscale instance to S3-compatible object storage and create new instances from such an archive. The model follows [CloudNativePG](https://cloudnative-pg.io/documentation/current/backup/):

- `spec.backup` only **writes** to an archive.
- `spec.bootstrap` decides, **once at creation**, whether an instance starts empty (`new`, the default) or is **recovered** from an archive listed in `spec.external_backups`.
- Archives in `spec.external_backups` are only ever **read**.

#### How It Works

With `spec.backup` set, two sidecars run next to Headscale:

| Sidecar | What it archives | How |
|---------|------------------|-----|
| `litestream` | the SQLite database | [Litestream](https://litestream.io) ships every WAL change (each `sync_interval`, 1s by default) plus periodic snapshots |
| `backup-agent` | `noise.private_key_path` and, with embedded DERP, `derp.server.private_key_path` | uploads each key once (and again only if it changes) with a checksum manifest, and every 30s writes a heartbeat with Litestream's progress |

The private keys matter as much as the database: the noise key **is the server's identity**. A database restored without it gives the server a new identity, and every client has to re-register. Archiving both keeps one archive self-sufficient for a recovery:

```
s3://<bucket>/<prefix>/
├── db/                       # Litestream replica of the database
├── keys/noise.key            # headscale private keys
├── keys/derp.key
├── keys/manifest.json        # checksums of the archived keys
├── agent/heartbeat.json      # replication progress, reported from inside the pod
└── .headscale-operator.json  # which Headscale instance owns this archive
```

Both sidecars are [native sidecars](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/): they start before Headscale and stop after it, so Litestream sees the database from its first write and ships the last writes on shutdown.

**Requirements**

- Kubernetes 1.29 or newer (native sidecars)
- Headscale v0.27.1 or newer: older releases refuse to start on a database Litestream has replicated (they reject its `_litestream_*` tables, see [juanfont/headscale#2843](https://github.com/juanfont/headscale/pull/2843)). Older versions are reported as `UnsupportedVersion`
- `replicas: 1` (enforced by the CRD)
- `database.type: sqlite` with write-ahead logging (Headscale's default), with the database under `/var/lib/headscale`
- Keys stored outside `/var/lib/headscale` (for example mounted from a Secret) are considered managed by you and are neither archived nor restored
- Static S3 credentials in a Secret of the Headscale's namespace. The sidecars, the recovery Job and the operator all use them; ambient cloud identities (IRSA, EKS Pod Identity, ...) are not supported
- The object store must be reachable from the operator as well (it claims and inspects archives). For an in-cluster store, use the fully qualified service name (`<service>.<namespace>.svc`) in `endpoint_url`

#### Enabling Continuous Backup

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: Headscale
metadata:
  name: headscale
  namespace: headscale
spec:
  # ...
  backup:
    object_store:
      destination_path: s3://my-bucket/headscale/prod   # must not be used by any other instance
      # endpoint_url: http://minio.minio.svc:9000      # for S3-compatible stores; in-cluster: use the FQDN
      region: eu-central-1
      s3_credentials:
        access_key_id:
          name: headscale-backup-s3
          key: access-key-id
        secret_access_key:
          name: headscale-backup-s3
          key: secret-access-key
    retention_policy: 720h        # how long recovery points are kept (default 30 days)
    litestream:
      sync_interval: 1s           # bounds the data loss window (default 1s)
      snapshot_interval: 24h      # full snapshots; shorter means faster restores (default 24h)
      # image: litestream/litestream:0.5.17
      # resources: {...}
    # agent:
    #   image: ...                # defaults to the backup-agent image of the operator release
    #   resources: {...}
```

Enabling (or disabling) backup on an existing instance changes its pod template, so the Headscale pod restarts once. Instances without `spec.backup` are left exactly as they are.

A complete example is in [config/samples/headscale_v1beta1_headscale_backup.yaml](config/samples/headscale_v1beta1_headscale_backup.yaml).

#### Backup Status

```sh
kubectl get headscale -n headscale
# NAME        READY   ARCHIVING   AGE
# headscale   True    True        30d

kubectl get headscale headscale -n headscale -o jsonpath='{.status.backup}' | jq
# {
#   "destination_path": "s3://my-bucket/headscale/prod",
#   "endpoint_url": "https://minio.storage.svc:9000",
#   "first_recoverability_point": "2026-10-01T08:00:00Z",
#   "last_successful_backup": "2026-10-18T12:00:01Z",
#   "keys_archived": true
# }
```

The `ContinuousArchiving` condition is `True` once the sidecars run, a base snapshot is in the archive, all keys are archived and the backup-agent's heartbeat shows that the pod keeps up. The heartbeat matters because the operator and the pod reach the archive independently: the pod keeps the credentials it started with and has its own network path, so an archive the operator can read says nothing about the pod still writing to it. The agent compares Litestream's position in the local database with the newest transaction in the archive. An idle database is not lagging, so a quiet instance never looks stale; a busy one only counts as lagging while the archive fails to reach a position the database had reached earlier, so being a few transactions behind at every check is not a stall. Otherwise the reason says why:

| Reason | Meaning |
|--------|---------|
| `SidecarNotReady` | The `litestream` or `backup-agent` sidecar is not running yet |
| `NoBaseSnapshot` | Waiting for Litestream to upload the first snapshot |
| `KeysNotArchived` | Waiting for Headscale to generate, or the agent to upload, a private key |
| `AgentNotReporting` | No heartbeat from the backup-agent for 3 minutes: the pod cannot write to the archive (revoked or rotated credentials, network, permissions) |
| `ReplicationStalled` | The archive has not reached a transaction the local database had for longer than `max(2m, 3 × sync_interval)` |
| `ReplicationUnknown` | The agent cannot read Litestream's metrics |
| `Suspended` (`Unknown`) | `replicas: 0`: no pod runs, so nothing is archived |
| `CredentialsMissing` | The credentials Secret or key does not exist |
| `ObjectStoreUnreachable` | The archive could not be read or written |
| `ArchiveInUse` | Another Headscale instance owns `destination_path` on this endpoint. An instance that loses its claim stops archiving: its sidecars are removed |
| `UnsupportedDatabase`, `WALDisabled`, `MissingSetting`, `UnsupportedPath`, `PortConflict`, `UnsupportedVersion` | `spec.config` or `spec.version` cannot be archived (see Requirements) |

Backup problems never take Headscale down: the sidecars have no readiness probes and keep retrying, and once an archive is claimed, credential or object store errors are only reported, never acted upon. The one exception is losing the archive to another instance, which removes the sidecars: two writers would corrupt the archive for both.

#### Recovering an Instance

Recovery always creates a **new** Headscale resource, like a CNPG cluster bootstrapped from a backup:

```yaml
apiVersion: headscale.infrado.cloud/v1beta1
kind: Headscale
metadata:
  name: headscale-restored
  namespace: headscale
spec:
  # ... same config as the source instance ...
  bootstrap:
    recovery:
      source: prod
      recovery_target:                       # omit to recover the latest state
        target_time: "2026-10-10T08:00:00Z"
  external_backups:
    - name: prod
      object_store:
        destination_path: s3://my-bucket/headscale/prod
        region: eu-central-1
        s3_credentials: { ... }
  backup:                                    # optional: keep archiving, to a new path
    object_store:
      destination_path: s3://my-bucket/headscale/prod-v2
      # ...
```

Before Headscale first starts, the operator:

1. checks the source: it must contain a database snapshot and the noise key;
2. creates the data PVC `data-<name>-0` exactly as the StatefulSet would;
3. runs the Job `<name>-full-recovery`: `litestream restore` (latest, or as of `target_time`) to a staging file, then the backup-agent verifies the keys against the manifest and moves the keys and finally the database into place;
4. deletes the Job and creates the StatefulSet.

Progress is reported on the `Bootstrapped` condition (and `Ready` is `False` with reason `Bootstrapping` meanwhile):

| Reason | Meaning |
|--------|---------|
| `NewInstance` | `bootstrap.new` or no bootstrap: Headscale initializes an empty instance |
| `NotRequired` | The instance already had its data (all instances created before this feature) |
| `RecoveryRunning` | The recovery Job is running. A container that cannot start (missing Secret, image pull error) is named in the message. A Job running for more than an hour fails |
| `Recovered` | Recovery completed; the message names the source and target time |
| `RecoveryFailed` | The Job failed; the message carries the Litestream or agent error. The Job is kept for inspection: **delete it to retry** |
| `NoRecoverableBackup`, `KeysMissing` | The source has no snapshot, or no noise key |
| `TargetOutOfRange` | `recovery_target.target_time` is before the source's oldest recovery point |
| `StatefulSetConflict` | A StatefulSet of the same name, not owned by this resource (left over from a deleted one), exists. Delete it and its PVC to recover |
| `UnsupportedVersion`, `UnsupportedDatabase`, `MissingSetting`, `UnsupportedPath` | The new instance cannot run on the recovered database (see Requirements) |
| `PVCAlreadyExists` | `data-<name>-0` already exists; recovery never writes into existing data. Delete the PVC to recover into a fresh volume |

`bootstrap` is immutable: it cannot be added to, changed on or removed from an existing resource.

A complete example is in [config/samples/headscale_v1beta1_headscale_recovery.yaml](config/samples/headscale_v1beta1_headscale_recovery.yaml).

#### Safety Guarantees

- **One writer per archive.** The first instance archiving to a path records itself in `.headscale-operator.json`, written with a conditional put (`If-None-Match`). Some S3-compatible stores ignore the condition and overwrite. To cover them, the operator reads the marker back a few seconds after writing it, so only the last of two racing writers wins. The claim is checked again on every reconcile. Any other instance pointed at the archive reports `ArchiveInUse` and does not archive, and an instance whose marker is replaced stops archiving. An archive is a path on an endpoint: the same path on another endpoint is claimed separately.
- **The recovery source is read-only.** The CRD rejects a `backup.object_store.destination_path` that equals, contains or is contained in the recovery source path (on the same endpoint), so a recovered instance always starts a fresh archive.
- **Existing data is never overwritten.** Recovery only writes into a PVC it created itself, and the agent refuses to replace an existing database.
- **No silent identity change.** A recovery fails rather than proceed without the noise key. (The DERP key is optional: clients learn a new one from the DERP map.)
- **Restartable restores.** The agent records the recovery next to the database before moving anything into place, and moves the database last. A failed recovery never leaves a volume that looks initialized, and an interrupted one completes when the Job retries.

#### Securing the Archive

The archive holds Headscale's database and its private keys, which is everything needed to impersonate the server. The operator does not encrypt the archive: protecting it is the object store's job. Treat the bucket accordingly:

- Enable server-side encryption on the bucket (SSE-S3 or SSE-KMS).
- Give each instance credentials limited to its prefix. For AWS S3:

  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::my-bucket",
       "Condition": {"StringLike": {"s3:prefix": ["headscale/prod/*"]}}},
      {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
       "Resource": "arn:aws:s3:::my-bucket/headscale/prod/*"}
    ]
  }
  ```

  Recovery only needs `s3:ListBucket` and `s3:GetObject` on the source prefix.
- Don't let bucket lifecycle rules expire `<path>/keys/` or `<path>/.headscale-operator.json`. Litestream applies `retention_policy` to the database itself. The keys are written once and never rewritten, so an age-based rule deletes them first. The backup-agent re-uploads a deleted key while the pod runs, and the `KeysNotArchived` reason reports it in the meantime. But an archive whose instance is gone can no longer be recovered with its server identity.

#### Monitoring Backups

Watch the `ContinuousArchiving` condition: it is the one signal that covers the pod's own path to the archive (see [Backup Status](#backup-status)). Every change of its reason is also recorded as an event on the Headscale resource, as a `Warning` for real problems.

While backup is enabled, the `<name>-metrics` Service also exposes Litestream's Prometheus metrics on port `9091` (named `litestream`). Litestream 0.5 does not count failed uploads, so use its metrics to complement the condition, not replace it:

```yaml
- alert: HeadscaleBackupWALGrowing    # the WAL keeps growing while uploads fail
  expr: litestream_wal_size > 64 * 1024 * 1024
- alert: HeadscaleBackupSyncErrors    # Litestream cannot read the local database
  expr: increase(litestream_sync_error_count[10m]) > 0
```

#### Operating Notes

- **Rotating the S3 credentials.** The sidecars read the Secret when the pod starts. After changing it, restart the pod (`kubectl rollout restart statefulset/<name>`). Until then the condition reports `AgentNotReporting` if the old keys stop working.
- **Re-creating a Headscale resource** (delete and create, a GitOps re-sync after a cluster rebuild, a Velero restore) creates a new instance with a new UID. It reports `ArchiveInUse` on the old path, by design: point `backup` at a new path, or recover from the old archive with `bootstrap.recovery`. If the previous instance is gone for good and you want to keep writing to the same path, delete `<path>/.headscale-operator.json` yourself.
- **Changing `destination_path` or `endpoint_url`** claims the new archive. Until that succeeds, nothing is archived: the sidecars are removed and `status.backup` is cleared.
- **`external_backups` entries used by `bootstrap.recovery` cannot be removed**, since `bootstrap` is immutable and must keep pointing at an existing source. Keep the entry (and its Secret reference) for the lifetime of the resource.

#### Disaster Recovery Runbook

1. Make sure the old instance is gone or stopped, so it no longer serves clients.
2. Create a new Headscale resource with the same `spec.config`, `bootstrap.recovery` pointing at the archive, and `backup` pointing at a **new** path.
3. Wait for `Bootstrapped=True` and `Ready=True`:
   `kubectl wait headscale/<name> --for=condition=Ready --timeout=10m`
4. Point `server_url`'s DNS (or your Ingress) at the new instance. Clients reconnect with their existing node keys, because the server identity was recovered.
5. Rehearse this regularly: a backup is only as good as the last successful restore.

#### Limitations

- Only the SQLite backend is supported. For PostgreSQL, use your database's own backup tooling (for example CloudNativePG).
- The sidecar images must be pullable for the Headscale pod to start (as for any container of the pod).
- During a long object store outage the WAL file keeps growing, since Litestream cannot ship it.
- Point-in-time recovery precision is bounded by `sync_interval`.

### Uninstallation

Delete the Headscale instance:

```sh
kubectl delete -k config/samples/
```

To completely remove the operator:

```sh
helm uninstall headscale-operator
```

## Development

### Building from Source

```sh
# Build the operator binary
make build

# Run tests
make test

# Build Docker images (operator, apikey-manager and backup-agent)
make docker-build IMG=<registry>/headscale-operator:tag
make docker-build-backup-agent BACKUP_AGENT_IMG=<registry>/headscale-operator/backup-agent:tag
```

### Running Locally

```sh
# Install CRDs
make install

# Run the operator locally (outside the cluster)
make run

# In another terminal, create a sample Headscale instance
kubectl apply -f config/samples/headscale_v1beta1_headscale.yaml
```

### Deploying to Cluster

```sh
# Build and push image
make docker-build docker-push IMG=<registry>/headscale-operator:tag

# Deploy to cluster
make deploy IMG=<registry>/headscale-operator:tag
```

### Running Tests

```sh
# Run unit tests
make test

# Run end-to-end tests (creates and deletes a dedicated Kind cluster)
make test-e2e
```

The end-to-end tests install and finally uninstall the CRDs, which deletes every `Headscale` resource of the cluster they run against. They therefore refuse to run unless the current kubectl context is the Kind cluster created for them (`kind-headscale-operator-test-e2e`). Keep your regular kubeconfig out of the way, for example:

```sh
KUBECONFIG=$(mktemp) make test-e2e
```

The object store tests can run against any S3-compatible server:

```sh
OBJECTSTORE_TEST_ENDPOINT=http://127.0.0.1:7070 OBJECTSTORE_TEST_BUCKET=test \
  AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... go test ./internal/objectstore/
```

## Contributing

We welcome contributions! Here's how you can help:

1. **Fork the repository** and create your branch from `main`
2. **Make your changes** and add tests if applicable
3. **Ensure tests pass** by running `make test`
4. **Format your code** with `make fmt` and `make vet`
5. **Commit your changes** using conventional commits
6. **Open a pull request** with a clear description of your changes

## Acknowledgments

- [Headscale](https://github.com/juanfont/headscale) - The awesome project this operator manages
- [Kubebuilder](https://book.kubebuilder.io/) - The framework used to build this operator
- All our [contributors](https://github.com/infradohq/headscale-operator/graphs/contributors)

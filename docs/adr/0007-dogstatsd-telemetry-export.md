# ADR-0007: DogStatsD Telemetry Export to Datadog

## Status

Accepted

## Context

Helix currently emits only `tracing` logs to stdout. For the "Dark Factory"
demo, a 3-node Helix cluster runs on GKE under load and an observer agent needs
to read production metrics (produce latency, commit latency, replication lag)
from Datadog to detect improvement opportunities.

The target GKE cluster (`gke_datadog-sandbox_us-west3_gs-us-west3`) is a shared,
multi-tenant sandbox running a Datadog Agent DaemonSet (`gensim-datadog`, ns
`datadog`). Inspection of that DaemonSet shows:

- **DogStatsD: enabled** — port 8125, `DD_DOGSTATSD_NON_LOCAL_TRAFFIC=true`,
  UDS at `/var/run/datadog/dsd.socket`.
- **APM/traces: enabled** — port 8126, `DD_APM_NON_LOCAL_TRAFFIC=true`.
- **OTLP receiver: disabled** — no 4317/4318 ports exposed; only
  `DD_OTLP_CONFIG_LOGS_ENABLED=false` is present.

Reconfiguring the shared DaemonSet to enable an OTLP receiver would risk
disrupting other tenants on the cluster, which is out of scope.

## Decision

Helix exports metrics to Datadog via **DogStatsD**, sending UDP packets to the
node-local Datadog Agent on port 8125.

- Latency metrics use **DogStatsD histograms** (`:<v>|h`); the local agent
  computes `.avg`/`.max`/`.median`/`.95percentile`/`.count`. (We initially used
  distributions `|d` for server-side global percentiles, but this cluster's
  Datadog org does not have distribution ingestion enabled, so `|d` packets were
  silently dropped. Histograms are the proven path here — every other app on the
  cluster uses them. See the telemetry-path note below.)
- A new `metrics` module in `helix-server` owns a small DogStatsD client and a
  typed surface for the demo signals:
  - `helix.produce.latency_ms` (distribution) — produce request → ack.
  - `helix.commit.latency_ms` (distribution) — propose → raft apply.
  - `helix.replication.lag` (gauge) — leader commit index − follower apply index.
- Every metric is tagged `service:helix`, `node_id:<n>`, `cluster:<id>`, and
  where relevant `topic:<t>`.
- Configuration is **environment-driven and opt-in**: the exporter reads
  `DD_AGENT_HOST` (set in k8s via a `fieldRef` to the node's host IP) and
  `DD_DOGSTATSD_PORT` (default 8125). When `DD_AGENT_HOST` is unset (local dev,
  tests, simulation), the exporter is a **no-op** that never opens a socket.

The emitter lives at the server I/O boundary, not inside the deterministic
Raft/WAL core, so it introduces no wall-clock or nondeterminism into
simulation-visible code paths.

## Consequences

**Easier:**
- No change to shared cluster infrastructure; zero blast radius for other tenants.
- DogStatsD is already accepted by the agent, so the path is known-good.
- Distributions give accurate cross-node percentiles for the demo's hero graph.
- No-op-when-unset keeps unit tests and DST runs free of network I/O.

**More difficult / trade-offs:**
- DogStatsD over UDP is best-effort (packets can drop under load); acceptable
  for an observability signal, not for billing-grade accuracy.
- Metric names/tags become a contract the observer agent depends on; renaming
  them later breaks the agent's queries.
- Distributions cost more in Datadog than plain histograms; fine at demo scale.

## Options Considered

### Option 1: DogStatsD distributions to the node agent (chosen)

**Pros:**
- Shared agent already accepts DogStatsD on 8125 with non-local traffic on.
- No shared-infra reconfiguration; no cluster-admin needed.
- Server-side percentiles via distributions.

**Cons:**
- UDP best-effort delivery.
- Adds a (small) statsd client dependency to `helix-server`.

### Option 2: OTLP from Helix to the node agent

**Pros:**
- Keeps Helix instrumentation vendor-neutral / OTLP-native.

**Cons:**
- The shared agent's OTLP receiver is disabled; enabling it edits a DaemonSet
  other tenants rely on — disruptive and out of scope.

### Option 3: OTLP to a sidecar OpenTelemetry Collector in our namespace

**Pros:**
- Helix stays OTLP-native; self-contained in our namespace.

**Cons:**
- Extra component to run and a Datadog API-key secret to manage.
- More moving parts to fail during a live demo than a direct statsd send.

## Telemetry path on the shared GKE cluster (gs-us-west3)

Two cluster-specific facts the deployment must honor:

1. **Reach the agent via the cluster Service DNS, not the node IP.** The shared
   agent DaemonSet does not expose a hostPort for DogStatsD 8125 (only a
   containerPort + host UDS). `DD_AGENT_HOST=status.hostIP` silently drops all
   packets. Instead set `DD_AGENT_HOST=gensim-datadog.datadog.svc.cluster.local`
   — a ClusterIP Service exposes 8125/UDP and load-balances to agent pods. This
   is the pattern every other app on the cluster uses.
2. **Use histograms (`|h`), not distributions (`|d`).** This org has not enabled
   distribution ingestion, so `|d` is dropped. `|h` is locally aggregated by the
   agent and works.

Both were verified by inspecting how `dogbank`/`gensim-*` apps emit metrics.

## References

- Plan: `temper/docs/dark-factory-helix-plan.md` (telemetry contract).
- Datadog Agent DaemonSet `gensim-datadog` in ns `datadog` (DogStatsD/APM on,
  OTLP off) — confirmed via `kubectl get daemonset gensim-datadog -n datadog`.
- `gensim-datadog` ClusterIP Service in ns `datadog` exposes 8125/UDP + 8126/TCP.

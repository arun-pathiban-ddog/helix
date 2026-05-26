//! DogStatsD metrics export.
//!
//! Helix emits a small set of demo-relevant signals to a node-local Datadog
//! Agent over the DogStatsD UDP protocol (see ADR-0007). The agent then
//! computes server-side percentiles for distribution metrics.
//!
//! # Configuration
//!
//! The exporter is environment-driven and opt-in:
//!
//! - `DD_AGENT_HOST` — host of the Datadog Agent. In Kubernetes this is set via
//!   a `fieldRef` to the node's host IP (`status.hostIP`). **When unset, the
//!   exporter is a no-op and never opens a socket** — local dev, unit tests,
//!   and deterministic simulation pay no cost and perform no I/O.
//! - `DD_DOGSTATSD_PORT` — UDP port of the agent's DogStatsD listener
//!   (default `8125`).
//!
//! # Wire format
//!
//! Each datagram is `name:value|<type>|#tag1:v1,tag2:v2` (UTF-8). Distributions
//! use type `d`; gauges use `g`. See
//! <https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/>.
//!
//! # Why a hand-rolled UDP client
//!
//! The DogStatsD datagram is a trivial text format. Sending it over
//! `std::net::UdpSocket` avoids a new third-party dependency, keeps
//! `#![forbid(unsafe_code)]` intact, and keeps the emit path allocation-light
//! and non-blocking (sends are best-effort and never block the caller).
//!
//! This module lives at the server I/O boundary. It does NOT run inside the
//! deterministic Raft/WAL core, so it introduces no wall-clock reads or
//! nondeterminism into simulation-visible code paths.

use std::fmt::Write as _;
use std::net::UdpSocket;

/// Metric name: produce request received to client acknowledgement, in ms.
pub const METRIC_PRODUCE_LATENCY_MS: &str = "helix.produce.latency_ms";

/// Metric name: Raft propose to applied (commit) latency, in ms.
pub const METRIC_COMMIT_LATENCY_MS: &str = "helix.commit.latency_ms";

/// Metric name: leader commit index minus follower apply index (entries).
pub const METRIC_REPLICATION_LAG: &str = "helix.replication.lag";

/// Metric name: records returned to a consumer per Fetch (count).
///
/// Server-side, so it counts EVERY consumer's reads — governed workloads and
/// any other Kafka client alike — the consume-side counterpart to
/// `helix.produce.latency_ms`.
pub const METRIC_CONSUME_FETCHED: &str = "helix.consume.fetched";

/// Metric name: time to serve a Fetch request, in ms (server-side).
pub const METRIC_CONSUME_FETCH_LATENCY_MS: &str = "helix.consume.fetch_latency_ms";

/// Maximum bytes for a single `DogStatsD` datagram.
///
/// `DogStatsD` over UDP fits comfortably within one packet for our metric
/// names and tag sets. We bound the buffer rather than grow unbounded
/// (`TigerStyle`).
const DATAGRAM_BYTES_MAX: usize = 512;

/// A `DogStatsD` metric exporter bound to a node-local agent.
///
/// Cloneable and cheap to share across tasks: the underlying UDP socket is
/// connectionless and safe to send from concurrently. When the exporter is
/// disabled (no `DD_AGENT_HOST`), all emit calls return immediately.
#[derive(Clone)]
pub struct Metrics {
    inner: Option<std::sync::Arc<Inner>>,
}

struct Inner {
    socket: UdpSocket,
    /// Constant tag suffix shared by every metric, e.g.
    /// `|#service:helix,cluster:helix-docker,node_id:1`. Built once.
    base_tags: String,
}

impl Metrics {
    /// Builds a metrics exporter from the environment.
    ///
    /// Returns a **disabled** exporter (no socket, all emits are no-ops) when
    /// `DD_AGENT_HOST` is unset. Returns a disabled exporter (and logs a
    /// warning) if the socket cannot be created or connected, so telemetry
    /// failures never take down the server.
    ///
    /// `node_id` and `cluster_id` are attached as tags to every metric.
    ///
    /// # Panics
    ///
    /// Panics if `cluster_id` is empty; a running server always has one.
    #[must_use]
    pub fn from_env(node_id: u64, cluster_id: &str) -> Self {
        // Precondition: a cluster id is always present in a running server.
        assert!(!cluster_id.is_empty(), "cluster_id cannot be empty");

        let Ok(host) = std::env::var("DD_AGENT_HOST") else {
            tracing::info!("DD_AGENT_HOST unset; metrics export disabled");
            return Self { inner: None };
        };
        if host.is_empty() {
            tracing::info!("DD_AGENT_HOST empty; metrics export disabled");
            return Self { inner: None };
        }

        let port: u16 = std::env::var("DD_DOGSTATSD_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8125);

        match Self::connect(&host, port, node_id, cluster_id) {
            Ok(inner) => {
                tracing::info!(host, port, node_id, "DogStatsD metrics export enabled");
                Self {
                    inner: Some(std::sync::Arc::new(inner)),
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, host, port, "metrics export disabled: socket setup failed");
                Self { inner: None }
            }
        }
    }

    /// Returns a permanently-disabled exporter whose emit calls are no-ops and
    /// which never opens a socket. Used by test and simulation constructors.
    #[must_use]
    pub const fn disabled() -> Self {
        Self { inner: None }
    }

    /// Returns `true` when the exporter is active (a socket is open and emits
    /// will be sent). `false` when disabled (all emits are no-ops).
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Creates and connects the UDP socket, building the shared tag suffix.
    fn connect(host: &str, port: u16, node_id: u64, cluster_id: &str) -> std::io::Result<Inner> {
        // Bind to an ephemeral local port on all interfaces; connect() fixes the
        // destination so subsequent send() calls take no address.
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.connect((host, port))?;
        // Non-blocking: a full socket buffer drops the packet rather than
        // blocking the hot path. Telemetry is best-effort.
        socket.set_nonblocking(true)?;

        let base_tags = format!("|#service:helix,cluster:{cluster_id},node_id:{node_id}");

        // Postcondition: the tag suffix is well-formed.
        assert!(base_tags.starts_with("|#"), "base_tags must start with |#");
        Ok(Inner { socket, base_tags })
    }

    /// Records a histogram sample (e.g. a latency in milliseconds).
    ///
    /// Emitted as a `DogStatsD` histogram (`|h`): the local agent computes and
    /// submits `.avg`, `.max`, `.median`, `.95percentile`, and `.count` as
    /// gauges. We use histogram rather than distribution (`|d`) because this
    /// cluster's Datadog org does not have distribution ingestion enabled, so
    /// `|d` packets are silently dropped — `|h` is the proven path here.
    /// Optional `extra_tags` are appended as `key:value` pairs.
    pub fn histogram(&self, name: &str, value: f64, extra_tags: &[(&str, &str)]) {
        self.emit(name, value, 'h', extra_tags);
    }

    /// Records a gauge value (e.g. current replication lag in entries).
    pub fn gauge(&self, name: &str, value: f64, extra_tags: &[(&str, &str)]) {
        self.emit(name, value, 'g', extra_tags);
    }

    /// Increments a counter by `value` (`DogStatsD` `|c`), e.g. records consumed.
    pub fn count(&self, name: &str, value: f64, extra_tags: &[(&str, &str)]) {
        self.emit(name, value, 'c', extra_tags);
    }

    /// Formats and sends one datagram. No-op when disabled.
    fn emit(&self, name: &str, value: f64, kind: char, extra_tags: &[(&str, &str)]) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        // Precondition: a metric name is always provided by callers (constants).
        debug_assert!(!name.is_empty(), "metric name cannot be empty");

        let mut packet = String::with_capacity(DATAGRAM_BYTES_MAX);
        packet.push_str(name);
        packet.push(':');
        // DogStatsD values are plain decimals. Format to 3 decimal places to
        // avoid float-representation noise (e.g. 10.500000000000002) and any
        // exponent notation, which some agent parsers reject. write! into the
        // existing buffer avoids a throwaway String allocation.
        let _ = write!(packet, "{value:.3}");
        packet.push('|');
        packet.push(kind);
        packet.push_str(&inner.base_tags);
        for (k, v) in extra_tags {
            packet.push(',');
            packet.push_str(k);
            packet.push(':');
            packet.push_str(v);
        }

        // Drop oversized packets rather than send a malformed/truncated datagram.
        if packet.len() > DATAGRAM_BYTES_MAX {
            tracing::debug!(name, len = packet.len(), "dropping oversized statsd packet");
            return;
        }

        // Best-effort send: a WouldBlock or transient error is intentionally
        // ignored so telemetry never perturbs request handling.
        let _ = inner.socket.send(packet.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_exporter_is_noop() {
        // A disabled exporter (no socket) is what `from_env` returns when
        // DD_AGENT_HOST is unset. Construct it directly to avoid mutating
        // process env (which is `unsafe` under edition 2024 / forbid(unsafe_code))
        // and to keep the test independent of ambient environment.
        let m = Metrics { inner: None };
        assert!(m.inner.is_none(), "exporter must be disabled");
        // Emitting on a disabled exporter must not panic and must perform no I/O.
        m.histogram(METRIC_PRODUCE_LATENCY_MS, 1.5, &[("topic", "t")]);
        m.gauge(METRIC_REPLICATION_LAG, 3.0, &[]);
    }

    #[test]
    fn test_datagram_format_is_well_formed() {
        // Build an enabled exporter against a throwaway local socket so we can
        // observe exactly what bytes would be sent.
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
        let addr = receiver.local_addr().expect("addr");
        let socket = UdpSocket::bind("0.0.0.0:0").expect("bind sender");
        socket.connect(addr).expect("connect");
        let inner = Inner {
            socket,
            base_tags: "|#service:helix,cluster:helix-test,node_id:1".to_string(),
        };
        let m = Metrics {
            inner: Some(std::sync::Arc::new(inner)),
        };

        m.histogram(METRIC_PRODUCE_LATENCY_MS, 12.5, &[("topic", "orders")]);

        let mut buf = [0u8; DATAGRAM_BYTES_MAX];
        let n = receiver.recv(&mut buf).expect("recv");
        let got = std::str::from_utf8(&buf[..n]).expect("utf8");
        assert_eq!(
            got,
            "helix.produce.latency_ms:12.500|h|#service:helix,cluster:helix-test,node_id:1,topic:orders"
        );
    }
}

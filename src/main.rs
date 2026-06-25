//! speedcurl — an ultra-high-performance, zero-disk-I/O speedtest engine.
//!
//! Everything is served from and consumed into memory: the download path streams
//! slices of a single pre-generated buffer (cloning a `Bytes` is a refcount bump,
//! never a copy or allocation), and the upload path drains the request body and
//! drops each chunk the instant it is counted. Nothing ever touches the disk.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::time::Instant;

use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, RawQuery},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use tokio::net::TcpListener;

/// Size of each streamed slice. 256 KiB keeps syscall/framing overhead low while
/// staying friendly to the allocator and the network stack's write buffers.
const CHUNK_SIZE: usize = 256 * 1024;

/// Default download size when the client does not specify one (100 MiB).
const DEFAULT_DOWNLOAD_BYTES: u64 = 100 * 1024 * 1024;

/// Upper bound on a single download so one request can't stream unbounded (100 GiB).
const MAX_DOWNLOAD_BYTES: u64 = 100 * 1024 * 1024 * 1024;

/// Default listen port; overridable via the `PORT` environment variable.
const DEFAULT_PORT: u16 = 3220;

/// The master payload, generated once at first use. Filled with a deterministic
/// xorshift stream so the data is effectively incompressible — proxies and
/// gzip layers can't deflate it and skew the measured throughput.
static PAYLOAD: LazyLock<Bytes> = LazyLock::new(|| {
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for slot in buf.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *slot = (x & 0xff) as u8;
    }
    Bytes::from(buf)
});

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let app = Router::new()
        .route("/", get(index))
        .route("/ping", get(ping))
        .route("/download", get(download))
        .route("/upload", post(upload))
        // A speedtest exists to move large bodies; the default 2 MiB cap is fatal here.
        .layer(DefaultBodyLimit::disable());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    println!("speedcurl listening on http://{addr}");
    axum::serve(listener, app)
        .await
        .expect("server terminated unexpectedly");
}

/// Human-readable usage banner.
async fn index() -> Response {
    let body = concat!(
        "speedcurl — in-memory speedtest engine\n\n",
        "GET  /ping              latency probe, returns \"pong\"\n",
        "GET  /download?bytes=N  stream N bytes of incompressible data\n",
        "GET  /download?mb=N     stream N mebibytes (takes precedence over bytes)\n",
        "POST /upload            consume the request body and report throughput\n",
    );
    ([(header::CACHE_CONTROL, "no-store")], body).into_response()
}

/// Tiny, uncached latency probe.
async fn ping() -> Response {
    ([(header::CACHE_CONTROL, "no-store")], "pong").into_response()
}

/// Stream `bytes` (or `mb`) of incompressible data straight from memory.
///
/// Uses `unfold` over a remaining-byte counter, yielding zero-copy slices of the
/// shared [`PAYLOAD`] until the requested size is met. `Content-Length` is set so
/// clients can measure precisely and detect truncation.
async fn download(RawQuery(query): RawQuery) -> Response {
    let total = requested_size(query.as_deref());

    let body_stream = stream::unfold(total, |remaining| async move {
        if remaining == 0 {
            return None;
        }
        let n = remaining.min(CHUNK_SIZE as u64) as usize;
        let piece = PAYLOAD.slice(0..n);
        Some((Ok::<Bytes, Infallible>(piece), remaining - n as u64))
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, total)
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from_stream(body_stream))
        .expect("download response is always well-formed")
}

/// Drain the request body, counting bytes and discarding each chunk immediately,
/// then report the byte count and the implied upstream throughput.
async fn upload(body: Body) -> Response {
    let start = Instant::now();
    let mut stream = body.into_data_stream();
    let mut total: u64 = 0;

    while let Some(chunk) = stream.next().await {
        match chunk {
            // Counted, then dropped at end of scope — never buffered.
            Ok(data) => total += data.len() as u64,
            Err(_) => return (StatusCode::BAD_REQUEST, "upload stream error").into_response(),
        }
    }

    let secs = start.elapsed().as_secs_f64();
    let mbps = if secs > 0.0 {
        (total as f64 * 8.0) / 1_000_000.0 / secs
    } else {
        0.0
    };

    let body = format!("{{\"received_bytes\":{total},\"seconds\":{secs:.6},\"mbps\":{mbps:.3}}}");
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        body,
    )
        .into_response()
}

/// Resolve the requested download size from the raw query string.
///
/// Supports `?bytes=N` and `?mb=N` (mebibytes); `mb` wins if both are present.
/// The result is clamped to [`MAX_DOWNLOAD_BYTES`] and defaults to
/// [`DEFAULT_DOWNLOAD_BYTES`] when nothing parseable is supplied.
fn requested_size(query: Option<&str>) -> u64 {
    let mut bytes: Option<u64> = None;
    let mut mb: Option<u64> = None;

    if let Some(q) = query {
        for pair in q.split('&') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            match key {
                "bytes" => bytes = value.parse().ok(),
                "mb" => mb = value.parse().ok(),
                _ => {}
            }
        }
    }

    mb.map(|m| m.saturating_mul(1024 * 1024))
        .or(bytes)
        .unwrap_or(DEFAULT_DOWNLOAD_BYTES)
        .min(MAX_DOWNLOAD_BYTES)
}

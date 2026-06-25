//! speedcurl — an ultra-high-performance, zero-disk-I/O speedtest engine.
//!
//! Everything is served from and consumed into memory: the download path streams
//! slices of a single pre-generated buffer (cloning a `Bytes` is a refcount bump,
//! never a copy or allocation), and the upload path drains the request body and
//! drops each chunk the instant it is counted. Nothing ever touches the disk.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, RawQuery, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

/// Size of each streamed slice. 256 KiB keeps syscall/framing overhead low while
/// staying friendly to the allocator and the network stack's write buffers.
const CHUNK_SIZE: usize = 256 * 1024;

/// Default download size when the client does not specify one (100 MiB).
const DEFAULT_DOWNLOAD_BYTES: u64 = 100 * 1024 * 1024;

/// Upper bound on a single download so one request can't stream unbounded (100 GiB).
const MAX_DOWNLOAD_BYTES: u64 = 100 * 1024 * 1024 * 1024;

/// Default listen port; overridable via the `PORT` environment variable.
const DEFAULT_PORT: u16 = 3220;

/// Default cap on concurrently in-flight requests; overridable via `MAX_CONCURRENCY`.
/// Standalone (no proxy) means there is no upstream load shedder, so the engine
/// caps itself to keep an L7 flood from exhausting the Tokio worker pool / memory.
const DEFAULT_MAX_CONCURRENCY: usize = 100;

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

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {addr}: {e}"));

    println!(
        "speedcurl listening on http://{addr} (max {} concurrent requests)",
        max_concurrency()
    );
    axum::serve(listener, app())
        .await
        .expect("server terminated unexpectedly");
}

/// Build the speedcurl `Router` with the configured concurrency cap.
///
/// Extracted from `main` so the full routing + middleware stack can be exercised
/// in-process via `tower::ServiceExt::oneshot` — no sockets, no ports, no I/O.
fn app() -> Router {
    app_with_limit(max_concurrency())
}

/// Build the router with an explicit max in-flight request cap.
///
/// Separated out so tests can pin the limit deterministically (e.g. `0` to force
/// the saturated/shed-load path) without depending on the environment.
fn app_with_limit(max_concurrent: usize) -> Router {
    let limiter = Arc::new(Semaphore::new(max_concurrent));

    Router::new()
        .route("/", get(index))
        .route("/ping", get(ping))
        .route("/download", get(download))
        .route("/upload", post(upload))
        // A speedtest exists to move large bodies; the default 2 MiB cap is fatal here.
        .layer(DefaultBodyLimit::disable())
        // Outermost layer: shed load before any handler work begins.
        .layer(middleware::from_fn_with_state(limiter, limit_concurrency))
}

/// Resolve the concurrency cap from `MAX_CONCURRENCY`, falling back to the default.
fn max_concurrency() -> usize {
    std::env::var("MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_CONCURRENCY)
}

/// L7 DoS guard: bound the number of requests in flight at once.
///
/// `try_acquire` is non-blocking, so when the cap is reached we **shed load
/// instantly** with `503 Service Unavailable` instead of queueing the request.
/// Queueing under a flood would let connections pile up and exhaust memory and
/// the worker pool — the very thing this protects against. The permit is held for
/// the whole request and released automatically when it completes.
async fn limit_concurrency(
    State(limiter): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Response {
    match limiter.try_acquire() {
        Ok(_permit) => next.run(request).await,
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [
                (header::RETRY_AFTER, "1"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            "server at capacity\n",
        )
            .into_response(),
    }
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

#[cfg(test)]
mod tests {
    //! In-process integration tests.
    //!
    //! Every case drives the real [`app`] router through `tower::ServiceExt::oneshot`,
    //! so the full extractor/handler/middleware stack is exercised without binding a
    //! socket — fast, deterministic, and free of port contention in CI.

    use super::{DEFAULT_DOWNLOAD_BYTES, app, app_with_limit};
    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt; // brings `.oneshot()` into scope

    /// `/ping` is the latency probe. Clients derive round-trip latency from it, so
    /// it must reliably return `200 OK` with the exact, tiny body `pong`.
    #[tokio::test]
    async fn ping_returns_pong() {
        let response = app()
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"pong");
    }

    /// `/download?bytes=N` must stream *exactly* N bytes and advertise a matching
    /// `Content-Length`. Clients trust that header to measure throughput precisely
    /// and to detect a truncated transfer, so both must agree with the body.
    #[tokio::test]
    async fn download_honors_bytes_param() {
        let n = 4096usize;

        let response = app()
            .oneshot(
                Request::builder()
                    .uri(format!("/download?bytes={n}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            n.to_string().as_str()
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.len(), n);
    }

    /// `?mb=N` is mebibyte sugar and must take precedence over `bytes`. The 2 MiB
    /// size also spans multiple internal 256 KiB chunks, exercising the `unfold`
    /// streaming loop rather than a single slice. We pass a conflicting `bytes=1`
    /// to prove precedence is honored.
    #[tokio::test]
    async fn download_mb_takes_precedence_over_bytes() {
        let mb = 2u64;
        let expected = (mb * 1024 * 1024) as usize;

        let response = app()
            .oneshot(
                Request::builder()
                    .uri(format!("/download?bytes=1&mb={mb}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            expected.to_string().as_str()
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.len(), expected);
    }

    /// A bare `/download` with no size parameter must fall back to the documented
    /// default, so the simplest possible request is still a useful test. We assert
    /// on the header only and never drain the (100 MiB) lazy body, keeping the test
    /// cheap.
    #[tokio::test]
    async fn download_defaults_when_size_unspecified() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/download")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_LENGTH).unwrap(),
            DEFAULT_DOWNLOAD_BYTES.to_string().as_str()
        );
    }

    /// `/upload` must consume a streamed body without buffering it to disk and then
    /// report accurate metrics. We verify the byte count round-trips exactly and
    /// that the response is *valid JSON* carrying the documented fields — the whole
    /// point of the endpoint is a trustworthy machine-readable measurement.
    #[tokio::test]
    async fn upload_counts_bytes_and_returns_valid_json() {
        let payload = vec![0u8; 1024 * 1024]; // 1 MiB
        let len = payload.len() as u64;

        let response = app()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/upload")
                    .body(Body::from(payload))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("/upload must return valid JSON");

        assert_eq!(json["received_bytes"].as_u64().unwrap(), len);
        assert!(json["seconds"].as_f64().is_some(), "missing `seconds` metric");
        assert!(json["mbps"].as_f64().is_some(), "missing `mbps` metric");
    }

    /// When the concurrency cap is saturated the limiter must shed load *fast*:
    /// reply `503 Service Unavailable` with a `Retry-After` header rather than
    /// queueing the request. That fast rejection is what protects the standalone
    /// edge server from L7 connection-exhaustion floods. A zero-permit limiter is
    /// permanently saturated, so this exercises the rejection path deterministically.
    #[tokio::test]
    async fn concurrency_limit_sheds_load_with_503() {
        let response = app_with_limit(0)
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            response.headers().contains_key(header::RETRY_AFTER),
            "503 response must carry a Retry-After header"
        );
    }

    /// The limiter must be transparent under normal load: a request that fits
    /// within the cap reaches its handler unchanged (here, `/ping` still returns
    /// `200 pong`). Guards against the middleware accidentally blocking traffic.
    #[tokio::test]
    async fn request_passes_when_under_limit() {
        let response = app_with_limit(8)
            .oneshot(Request::builder().uri("/ping").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"pong");
    }
}

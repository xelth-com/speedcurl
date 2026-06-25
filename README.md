# speedcurl

Ultra-fast, zero-disk-I/O speedtest server. Download payloads stream straight from
RAM; uploads are counted and discarded on arrival. One small Rust binary (Axum + Tokio).

## Browser

Open `http://<host>:3220/` and hit **RUN TEST** — ping / download / upload, nothing to install.

## curl

```bash
curl http://<host>:3220/ping                            # latency -> pong
curl -o /dev/null "http://<host>:3220/download?mb=100"  # download 100 MiB
curl -o /dev/null "http://<host>:3220/download?bytes=1048576"

# upload (pipe from /dev/zero; @- reads stdin, avoids the arg-length limit)
head -c 50M /dev/zero | curl -X POST --data-binary @- http://<host>:3220/upload
# -> {"received_bytes":52428800,"seconds":...,"mbps":...}
```

Windows PowerShell upload (in-RAM, no temp file):

```powershell
$ProgressPreference='SilentlyContinue'
Invoke-RestMethod http://<host>:3220/upload -Method Post `
  -Body ([byte[]]::new(25MB)) -ContentType application/octet-stream
```

## Endpoints

| Method | Path | Notes |
|--------|------|-------|
| GET  | `/` | Browser speedtest UI |
| GET  | `/ping` | Returns `pong` |
| GET  | `/download?mb=N` (or `?bytes=N`) | Streams N from RAM (default 100 MiB, cap 100 GiB) |
| POST | `/upload` | Drains body → `{received_bytes, seconds, mbps}` |

## Config

| Env | Default | |
|-----|---------|--|
| `PORT` | `3220` | listen port |
| `MAX_CONCURRENCY` | `100` | max in-flight requests; excess get `503` |

## Build & run

```bash
cargo run               # serves http://0.0.0.0:3220
cargo build --release   # -> target/release/speedcurl
cargo test
```

## Deploy (standalone, no reverse proxy)

Build natively on the host, run under systemd, open the port:

```bash
git clone https://github.com/xelth-com/speedcurl.git && cd speedcurl
cargo build --release
sudo install -m 0755 target/release/speedcurl /usr/local/bin/
sudo install -m 0644 speedcurl.service /etc/systemd/system/
sudo systemctl enable --now speedcurl
sudo ufw allow 3220/tcp          # nftables: add `tcp dport 3220 accept`
curl http://localhost:3220/ping  # -> pong
```

`speedcurl.service` runs sandboxed as `nobody` (`ProtectSystem=strict`, `CAP_NET_BIND_SERVICE`
so `PORT` can move to 80/443 without root).

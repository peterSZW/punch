#!/usr/bin/env bash
# 把本机仓库同步到服务器，编译，并用 systemd 重启。
# 下次部署：./deploy.sh
# 主机写在环境变量 PUNCH_HOST，或写在未纳入版本库的 deploy.local.env。
# 已有的 yaml、证书和 punch-clients.json 不会被覆盖。
# 本机会先编出 macOS 和 Windows 客户端，服务器再编出 Linux 客户端，放进 dist/。

set -euo pipefail

DEST="${PUNCH_DEST:-/var/local/punch}"
CONTROL_PORT="${PUNCH_CONTROL_PORT:-6100}"
DATA_PORT="${PUNCH_DATA_PORT:-6101}"
WEB_PORT="${PUNCH_WEB_PORT:-6102}"

if [[ "${1:-}" == "--remote" ]]; then
  DEST="$2"
  CONTROL_PORT="$3"
  DATA_PORT="$4"
  WEB_PORT="$5"
  cd "$DEST"

  if ! rpm -q openssl-devel >/dev/null 2>&1 || ! command -v pkg-config >/dev/null 2>&1; then
    dnf install -y --disablerepo=mariadb gcc pkgconf-pkg-config openssl-devel
  fi

  if [[ -f "$HOME/.cargo/env" ]]; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
  fi
  if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain stable
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
  fi
  if ! cargo --version >/dev/null 2>&1; then
    rustup default stable
  fi

  mkdir -p "$HOME/.cargo"
  if [[ ! -f "$HOME/.cargo/config.toml" ]]; then
    cat > "$HOME/.cargo/config.toml" << 'EOF'
[source.crates-io]
replace-with = "rsproxy-sparse"

[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"
EOF
  fi

  port_busy() {
    ss -lnt | awk '{print $4}' | grep -Eq "(^|:)$1([^0-9]|$)"
  }

  if [[ ! -f "$DEST/punch-server.yaml" ]]; then
    for port in "$CONTROL_PORT" "$DATA_PORT" "$WEB_PORT"; do
      if port_busy "$port"; then
        echo "port $port is already in use" >&2
        exit 1
      fi
    done
    KEY="$(openssl rand -hex 24)"
    cat > "$DEST/punch-server.yaml" << YAML
control_listen: "0.0.0.0:${CONTROL_PORT}"
data_listen: "0.0.0.0:${DATA_PORT}"

api_key: "${KEY}"

web:
  listen: "0.0.0.0:${WEB_PORT}"
  cert: "certs/cert.pem"
  key: "certs/key.pem"

tcp:
  nodelay: true
  recv_buffer: 4194304
  send_buffer: 4194304
  pipe_size: 262144
  backlog: 4096

pool:
  idle: 8
  max_idle: 32
YAML
    chmod 600 "$DEST/punch-server.yaml"
    echo "PUNCH_API_KEY_CREATED=${KEY}"
  else
    echo "PUNCH_CONFIG_KEPT=${DEST}/punch-server.yaml"
  fi

  export CARGO_BUILD_JOBS=1
  export CARGO_PROFILE_RELEASE_LTO=false
  export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
  cargo build --release --locked

  mkdir -p "$DEST/dist"
  if [[ ! -f /usr/lib64/libssl.a || ! -f /usr/lib64/libcrypto.a ]]; then
    dnf install -y openssl-static >/dev/null 2>&1 || true
  fi
  if [[ -f /usr/lib64/libssl.a && -f /usr/lib64/libcrypto.a ]]; then
    PUNCH_OPENSSL_STATIC=1 OPENSSL_LIB_DIR=/usr/lib64 \
      CARGO_TARGET_DIR=/tmp/punch-static \
      cargo build --release --locked
    install -m 0755 /tmp/punch-static/release/punch "$DEST/dist/punch-linux"
  else
    echo "PUNCH_LINUX_DOWNLOAD=dynamic"
    install -m 0755 target/release/punch "$DEST/dist/punch-linux"
  fi

  install -d /etc/systemd/system
  cat > /etc/systemd/system/punch.service << UNIT
[Unit]
Description=punch reverse TCP tunnel
After=network.target

[Service]
Type=simple
WorkingDirectory=${DEST}
ExecStart=${DEST}/target/release/punch server -c ${DEST}/punch-server.yaml
Restart=on-failure
RestartSec=2
LimitNOFILE=1048576
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
UNIT

  systemctl daemon-reload
  systemctl enable punch
  systemctl restart punch
  sleep 1
  if ! systemctl is-active --quiet punch; then
    journalctl -u punch -n 40 --no-pager
    exit 1
  fi
  echo "PUNCH_ACTIVE=yes"
  ss -lnt | awk '{print $4}' | grep -E ":${CONTROL_PORT}([^0-9]|\$)|:${DATA_PORT}([^0-9]|\$)|:${WEB_PORT}([^0-9]|\$)" || true
  exit 0
fi

ROOT="$(cd "$(dirname "$0")" && pwd)"
if [[ -f "${ROOT}/deploy.local.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "${ROOT}/deploy.local.env"
  set +a
fi
HOST="${PUNCH_HOST:-}"
if [[ -z "$HOST" ]]; then
  echo "需要 PUNCH_HOST，例如 PUNCH_HOST=user@host ./deploy.sh" >&2
  echo "也可以写在 ${ROOT}/deploy.local.env，这个文件不要提交。" >&2
  exit 1
fi

build_client_downloads() {
  mkdir -p "$ROOT/dist"
  local mac_log win_log mac_pid win_pid failed=0
  mac_log="$(mktemp)"
  win_log="$(mktemp)"
  (
    cd "$ROOT"
    local ssl_lib="${OPENSSL_MAC_LIB:-/opt/homebrew/opt/openssl@3/lib}"
    if [[ ! -f "$ssl_lib/libssl.a" ]]; then
      echo "找不到 macOS 的静态 OpenSSL：$ssl_lib/libssl.a" >&2
      exit 1
    fi
    PUNCH_OPENSSL_STATIC=1 OPENSSL_LIB_DIR="$ssl_lib" \
      cargo build --release --locked
    cp -f target/release/punch dist/punch-macos
    chmod +x dist/punch-macos
  ) >"$mac_log" 2>&1 &
  mac_pid=$!
  (
    cd "$ROOT"
    export PATH="/opt/homebrew/opt/mingw-w64/bin:/opt/homebrew/bin:${PATH:-/usr/bin}"
    unset CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER || true
    if [[ ! -f /tmp/openssl-win/lib/libssl.a ]]; then
      echo "找不到 Windows 的静态 OpenSSL：/tmp/openssl-win/lib/libssl.a" >&2
      exit 1
    fi
    if ! command -v x86_64-w64-mingw32-gcc >/dev/null 2>&1; then
      echo "找不到 x86_64-w64-mingw32-gcc" >&2
      exit 1
    fi
    PUNCH_OPENSSL_STATIC=1 OPENSSL_LIB_DIR=/tmp/openssl-win/lib \
      CARGO_TARGET_DIR="$ROOT/target-windows" \
      cargo build --release --locked --target x86_64-pc-windows-gnu
    cp -f "$ROOT/target-windows/x86_64-pc-windows-gnu/release/punch.exe" "$ROOT/dist/punch-windows.exe"
  ) >"$win_log" 2>&1 &
  win_pid=$!
  if ! wait "$mac_pid"; then
    echo "macOS 客户端编译失败" >&2
    cat "$mac_log" >&2
    failed=1
  fi
  if ! wait "$win_pid"; then
    echo "Windows 客户端编译失败" >&2
    cat "$win_log" >&2
    failed=1
  fi
  rm -f "$mac_log" "$win_log"
  if [[ "$failed" -ne 0 ]]; then
    exit 1
  fi
}

build_client_downloads

echo "sync ${ROOT}/ -> ${HOST}:${DEST}/"
rsync -az --delete \
  --exclude '.git/' \
  --exclude 'target/' \
  --exclude 'target-windows/' \
  --exclude 'punch-server.yaml' \
  --exclude 'punch-clients.json' \
  --exclude '*.client.addr' \
  --exclude 'certs/' \
  --exclude 'deploy.local.env' \
  -e "ssh -o BatchMode=yes" \
  "${ROOT}/" "${HOST}:${DEST}/"

ssh -o BatchMode=yes "$HOST" \
  "bash $(printf '%q' "$DEST/deploy.sh") --remote $(printf '%q ' "$DEST" "$CONTROL_PORT" "$DATA_PORT" "$WEB_PORT")"

scp -o BatchMode=yes "$HOST:$DEST/dist/punch-linux" "$ROOT/dist/punch-linux"
chmod +x "$ROOT/dist/punch-linux"

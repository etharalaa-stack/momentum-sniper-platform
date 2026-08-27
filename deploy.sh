#!/usr/bin/env bash
# Frankfurt VPS — Momentum Sniper deployment
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
VENV="$ROOT/.venv"
LOG_DIR="$ROOT/logs"
ENV_FILE="$ROOT/.env"

echo "==> Momentum Sniper deploy (Frankfurt) — $ROOT"

# ── 1. System deps ──────────────────────────────────────────────────────────
if command -v apt-get &>/dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq python3 python3-venv python3-pip curl git tmux \
    build-essential libssl-dev 2>/dev/null || true
fi

# Node.js 20 LTS (if missing)
if ! command -v node &>/dev/null; then
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi

# PM2 (optional, preferred)
if ! command -v pm2 &>/dev/null; then
  sudo npm install -g pm2 2>/dev/null || npm install -g pm2 2>/dev/null || true
fi

# ── 2. Network tuning (low-latency WS) ──────────────────────────────────────
SYSCTL=/etc/sysctl.d/99-sniper.conf
if [ -w /etc/sysctl.d ] || sudo -n true 2>/dev/null; then
  sudo tee "$SYSCTL" >/dev/null <<'EOF'
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
net.ipv4.tcp_rmem = 4096 1048576 16777216
net.ipv4.tcp_wmem = 4096 1048576 16777216
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6
net.ipv4.tcp_fastopen = 3
net.core.netdev_max_backlog = 5000
EOF
  sudo sysctl --system 2>/dev/null || true
  echo "    [+] sysctl tuned for WebSocket streaming"
fi

# ── 3. Python venv + deps ───────────────────────────────────────────────────
python3 -m venv "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install -q --upgrade pip
pip install -q -r "$ROOT/requirements.txt"
echo "    [+] Python venv ready"

# ── 4. Environment ──────────────────────────────────────────────────────────
if [ ! -f "$ENV_FILE" ]; then
  cp "$ROOT/.env.example" "$ENV_FILE"
  echo "    [!] Created .env from .env.example — edit secrets before live use"
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

export SNIPER_DB="${SNIPER_DB:-$ROOT/sniper.db}"
export SNIPER_DRY_RUN="${SNIPER_DRY_RUN:-1}"
export SNIPER_BIND_HOST="${SNIPER_BIND_HOST:-0.0.0.0}"
export SNIPER_UVLOOP=1

# Write ports manifest for Next.js dashboard
mkdir -p "$ROOT/web/public"
API_URL="http://${SNIPER_BIND_HOST}:8787"
cat > "$ROOT/web/public/ports.json" <<EOF
{"api":{"host":"$SNIPER_BIND_HOST","port":8787,"url":"$API_URL"},"web":{"host":"$SNIPER_BIND_HOST","port":3000}}
EOF
export NEXT_PUBLIC_API_URL="$API_URL"
export NEXT_PUBLIC_DASHBOARD_KEY="${SNIPER_DASHBOARD_KEY:-dev-dashboard-key}"

cd "$ROOT/web"
npm ci --silent 2>/dev/null || npm install --silent
npm run build
cd "$ROOT"
echo "    [+] Next.js dashboard built"

# ── 6. Validate ─────────────────────────────────────────────────────────────
python "$ROOT/scripts/validate.py" || { echo "Validation failed"; exit 1; }

# ── 7. Process management ───────────────────────────────────────────────────
if command -v pm2 &>/dev/null; then
  pm2 delete sniper-engine sniper-api sniper-web 2>/dev/null || true
  pm2 start "$VENV/bin/python" --name sniper-engine --cwd "$ROOT" -- \
    "$ROOT/arb_engine.py"
  pm2 start "$VENV/bin/python" --name sniper-api --cwd "$ROOT" -- \
    "$ROOT/scripts/run_api.py"
  pm2 start npm --name sniper-web --cwd "$ROOT/web" -- start -- -p 3000 -H "$SNIPER_BIND_HOST"
  pm2 save
  echo "    [+] PM2 processes started (engine, api:8787, web:3000)"
else
  tmux kill-session -t sniper 2>/dev/null || true
  tmux new-session -d -s sniper -n engine \
    "$VENV/bin/python $ROOT/arb_engine.py"
  tmux new-window -t sniper -n api \
    "$VENV/bin/python $ROOT/scripts/run_api.py"
  tmux new-window -t sniper -n web \
    "cd $ROOT/web && npm start -- -p 3000"
  echo "    [+] tmux session 'sniper' started (attach: tmux attach -t sniper)"
fi

echo ""
echo "==> Deploy complete"
echo "    Engine:  SNIPER_DRY_RUN=$SNIPER_DRY_RUN  DB=$SNIPER_DB"
echo "    API:     http://$SNIPER_BIND_HOST:8787"
echo "    Dashboard: http://$SNIPER_BIND_HOST:3000"
echo "    Logs:    $LOG_DIR"

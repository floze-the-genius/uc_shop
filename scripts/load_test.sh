#!/usr/bin/env bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_stack
VENV="${VENV:-/tmp/uc-shop-load-venv}"
RPS="${RPS:-100}"
DURATION="${DURATION:-10}"
CONCURRENCY="${CONCURRENCY:-300}"
TIMEOUT="${TIMEOUT:-60}"
LATENCY="${PROVIDER_LATENCY:-0.005}"
PREFIX="${PREFIX:-script-load}"
DB_PASSWORD="${DB_PASSWORD:-$DB_USER}"

if [[ ! -x "$VENV/bin/python" ]]; then
  info "Создаю venv для load tools: $VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/python" -m pip install -q aiohttp asyncpg
fi

info "Load test: rps=$RPS duration=${DURATION}s concurrency=$CONCURRENCY"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$ROOT" "$VENV/bin/python" -m tools.load.main \
  --base-url "$API_URL" \
  --database-url "postgresql://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME" \
  --rps "$RPS" \
  --duration "$DURATION" \
  --concurrency "$CONCURRENCY" \
  --timeout "$TIMEOUT" \
  --provider-latency "$LATENCY" \
  --drain-timeout 180 \
  --reset \
  --prefix "$PREFIX"

wait_outbox_empty 30
STUCK="$(psqlq "SELECT count(*) FROM orders WHERE status NOT IN ('COMPLETED', 'FAILED');")"
assert_zero "$STUCK" "Остались незавершенные заказы"
ok "Load test прошел"

#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_URL="${API_URL:-http://localhost:8001}"
DB_CONTAINER="${DB_CONTAINER:-uc_shop-postgres-1}"
DB_NAME="${DB_NAME:-uc_shop}"
DB_USER="${DB_USER:-uc_shop}"

RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'
BLUE=$'\033[34m'; BOLD=$'\033[1m'; RESET=$'\033[0m'

info() { printf '%s==>%s %s\n' "$BLUE" "$RESET" "$*"; }
ok() { printf '%sOK%s %s\n' "$GREEN" "$RESET" "$*"; }
warn() { printf '%sWARN%s %s\n' "$YELLOW" "$RESET" "$*"; }
fail() { printf '%sFAIL%s %s\n' "$RED" "$RESET" "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null || fail "Не найдена команда: $1"
}

psqlq() {
  docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -Atc "$1"
}

post_order() {
  local key="$1" body="$2"
  curl -fsS -X POST "$API_URL/orders" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $key" \
    -d "$body"
}

wait_order_status() {
  local key="$1" expected="$2" timeout="${3:-40}" status=""
  for _ in $(seq 1 "$timeout"); do
    status="$(psqlq "SELECT status FROM orders WHERE idempotency_key = '$key';" || true)"
    [[ "$status" == "$expected" ]] && return 0
    sleep 1
  done
  fail "Заказ $key не дошел до $expected, текущий статус: ${status:-missing}"
}

assert_zero() {
  local value="$1" message="$2"
  [[ "$value" == "0" ]] || fail "$message: $value"
}

wait_outbox_empty() {
  local timeout="${1:-30}" pending=""
  for _ in $(seq 1 "$timeout"); do
    pending="$(psqlq "SELECT count(*) FROM order_events_outbox WHERE published_at IS NULL;")"
    [[ "$pending" == "0" ]] && return 0
    sleep 1
  done
  fail "Outbox не очистился, pending=$pending"
}

require_stack() {
  require_cmd docker
  require_cmd curl
  cd "$ROOT"
  docker compose ps >/dev/null
  curl -fsS "$API_URL/health" >/dev/null
}

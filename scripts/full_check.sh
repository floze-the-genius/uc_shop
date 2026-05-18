#!/usr/bin/env bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

cd "$ROOT"
require_cmd docker
require_cmd python3
require_cmd curl

info "Проверяю синтаксис Python и docker-compose"
python3 -m compileall -q src tools
find . -type d -name __pycache__ -prune -exec rm -rf {} +
docker compose config -q
ok "Статика прошла"

info "Поднимаю стенд"
ORDER_API_PORT="${ORDER_API_PORT:-8001}" ORDER_API_WORKERS="${ORDER_API_WORKERS:-6}" \
  docker compose up --build -d --scale outbox-relay=2 --scale order-processor=6

info "Жду API"
for _ in {1..60}; do
  curl -fsS "$API_URL/health" >/dev/null && break
  sleep 2
done
curl -fsS "$API_URL/health" >/dev/null || fail "API не поднялся"
ok "Стенд поднят"

"$ROOT/scripts/smoke_test.sh"
"$ROOT/scripts/retry_test.sh"
RPS="${RPS:-500}" DURATION="${DURATION:-10}" CONCURRENCY="${CONCURRENCY:-1200}" "$ROOT/scripts/load_test.sh"
RPS="${FAILOVER_RPS:-100}" DURATION="${FAILOVER_DURATION:-10}" CONCURRENCY="${FAILOVER_CONCURRENCY:-300}" "$ROOT/scripts/failover_test.sh"

LAG="$(docker exec uc_shop-kafka-1-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 --describe --group uc-shop-order-processors \
  | awk 'NR>1 && $6 ~ /^[0-9]+$/ {lag += $6} END {print lag+0}')"
assert_zero "$LAG" "Kafka lag не нулевой"

docker compose logs --since=2m order-api order-processor outbox-relay \
  | grep -E 'Traceback|CRITICAL|TooManyConnections|asyncpg.exceptions|RuntimeError' && fail "В логах есть ошибки" || true
ok "Full check прошел"

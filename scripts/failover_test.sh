#!/usr/bin/env bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_stack
BROKER="${BROKER:-uc_shop-kafka-2-1}"

cleanup() {
  docker start "$BROKER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

info "Останавливаю $BROKER"
docker stop "$BROKER" >/dev/null
sleep 6

info "Проверяю, что ISR остался минимум 2"
docker exec uc_shop-kafka-1-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe --topic orders.paid | sed -n '1,8p'

info "Гоню нагрузку при выключенном брокере"
RPS="${RPS:-100}" DURATION="${DURATION:-10}" CONCURRENCY="${CONCURRENCY:-300}" \
PREFIX="failover-$(date +%s)" "$ROOT/scripts/load_test.sh"

info "Возвращаю $BROKER"
docker start "$BROKER" >/dev/null
sleep 25
docker compose ps kafka-2
docker exec uc_shop-kafka-1-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 --describe --topic orders.paid | sed -n '1,8p'
ok "Failover test прошел"

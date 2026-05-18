#!/usr/bin/env bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_stack
KEY="smoke-$(date +%s)"
IDEM="idem-$(date +%s)"

info "Проверяю health"
curl -fsS "$API_URL/health"
printf '\n'

info "Создаю обычный заказ"
post_order "$KEY" '{"category":"GCRYSTALS","telegram_id":"smoke","payload":{"provider_latency":0.005}}' >/dev/null
wait_order_status "$KEY" COMPLETED 30
ok "Обычный заказ завершен"

info "Проверяю idempotency"
post_order "$IDEM" '{"category":"GCRYSTALS","telegram_id":"idem","payload":{"provider_latency":0.005}}' >/dev/null
post_order "$IDEM" '{"category":"GCRYSTALS","telegram_id":"idem","payload":{"provider_latency":0.005}}' >/dev/null
COUNT="$(psqlq "SELECT count(*) FROM orders WHERE idempotency_key = '$IDEM';")"
[[ "$COUNT" == "1" ]] || fail "Idempotency создал дубль: $COUNT"
wait_order_status "$IDEM" COMPLETED 30
ok "Idempotency работает"

wait_outbox_empty 30
ok "Smoke test прошел"

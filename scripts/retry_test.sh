#!/usr/bin/env bash
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

require_stack
OK_KEY="retry-ok-$(date +%s)"
DLQ_KEY="retry-dlq-$(date +%s)"

info "Создаю transient fail: должен пройти retry.1s и retry.3s"
post_order "$OK_KEY" '{"category":"GCRYSTALS","telegram_id":"retry-ok","payload":{"fail_until_attempt":2,"provider_latency":0.005}}' >/dev/null

info "Создаю permanent fail: должен уйти в DLQ"
post_order "$DLQ_KEY" '{"category":"GCRYSTALS","telegram_id":"retry-dlq","payload":{"force_fail":true}}' >/dev/null

wait_order_status "$OK_KEY" COMPLETED 40
wait_order_status "$DLQ_KEY" FAILED 40

OK_TOPICS="$(psqlq "SELECT string_agg(topic, ',' ORDER BY topic) FROM order_events_outbox WHERE aggregate_id = (SELECT id::text FROM orders WHERE idempotency_key = '$OK_KEY');")"
DLQ_TOPICS="$(psqlq "SELECT string_agg(topic, ',' ORDER BY topic) FROM order_events_outbox WHERE aggregate_id = (SELECT id::text FROM orders WHERE idempotency_key = '$DLQ_KEY');")"

[[ "$OK_TOPICS" == *orders.retry.1s* && "$OK_TOPICS" == *orders.retry.3s* ]] \
  || fail "Transient retry topics некорректны: $OK_TOPICS"
[[ "$DLQ_TOPICS" == *orders.retry.5s* && "$DLQ_TOPICS" == *orders.dlq* ]] \
  || fail "DLQ topics некорректны: $DLQ_TOPICS"

wait_outbox_empty 30
ok "Retry/DLQ test прошел"

#!/usr/bin/env bash
# kvreap on kind: the deploy/kserve kvreap component (radix eviction, KV events
# discovered through the headless Service) against a fake vLLM that writes a
# shared preamble, long-lived sessions and a stream of short ones. The fake
# vLLM starts first and its PUB stream is cut once mid-run through toxiproxy,
# so kvreap only sees the preamble and the cut batches by replaying them.
# Passes when kvreap prunes repeatedly while every live session and the
# preamble stay whole and no batch is lost.
#
#   tests/kind/e2e.sh            # KEEP=1 leaves the cluster up
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=${KIND_CLUSTER:-kvreap-e2e}
PRUNES=${PRUNES:-4}
TIMEOUT=${TIMEOUT:-600}
k() { kubectl --context "kind-$CLUSTER" -n kvreap-e2e "$@"; }

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
docker build -q --load -t kvreap:e2e .
docker build -q --load -t fake-vllm:e2e tests/kind/fake-vllm
docker pull -q ghcr.io/shopify/toxiproxy:2.12.0
kind load docker-image --name "$CLUSTER" kvreap:e2e fake-vllm:e2e ghcr.io/shopify/toxiproxy:2.12.0
kubectl --context "kind-$CLUSTER" delete namespace kvreap-e2e --ignore-not-found --wait=true
kubectl --context "kind-$CLUSTER" delete pv kvreap-e2e-kv-cache --ignore-not-found --wait=true
docker exec "$CLUSTER-control-plane" sh -c \
  'mkdir -p /kv-e2e && { mountpoint -q /kv-e2e || mount -t tmpfs -o size=1g,mode=1777 tmpfs /kv-e2e; } && rm -rf /kv-e2e/*'
kubectl --context "kind-$CLUSTER" apply -k tests/kind/overlay
k scale deployment/e2e-model-kvreap --replicas=0
k wait --for=delete pod -l app.kubernetes.io/component=kvreap --timeout=60s
k apply -f tests/kind/overlay/fake-vllm.yaml
k wait --for=condition=Ready pod/fake-vllm --timeout=120s
sleep 5
k scale deployment/e2e-model-kvreap --replicas=1
k rollout status deployment/e2e-model-kvreap --timeout=120s

log() { k logs deployment/e2e-model-kvreap 2>/dev/null || true; }
toxiproxy() { k exec fake-vllm -c toxiproxy -- /toxiproxy-cli "$@" >/dev/null; }
cut=0
deadline=$((SECONDS + TIMEOUT))
until [ "$(log | grep -c DELETION_END || true)" -ge "$PRUNES" ]; do
  if [ $SECONDS -ge $deadline ]; then
    log | tail -20
    echo "FAIL: fewer than $PRUNES prunes in ${TIMEOUT}s" >&2
    exit 1
  fi
  if [ $cut = 0 ] && log | grep -q DELETION_END; then
    toxiproxy toggle kv-events
    sleep 5
    toxiproxy toggle kv-events
    cut=1
  fi
  sleep 5
done

fail=0
pod_ip=$(k get pod fake-vllm -o jsonpath='{.status.podIP}')
logs=$(log)
case "$logs" in
  *"subscribed to KV cache events endpoint=\"tcp://$pod_ip:5557\""*) ;;
  *) echo "FAIL: kvreap never subscribed to the fake vLLM at $pod_ip" >&2; fail=1 ;;
esac

sleep 31
logs=$(log)
chains=$(printf '%s\n' "$logs" | grep ' chains ' | tail -1)
counter() { echo "$chains" | tr ' ' '\n' | sed -n "s/^$1=//p"; }
echo "$chains"
[ "$(counter event_batches)" -gt 0 ] || { echo "FAIL: no event batches" >&2; fail=1; }
[ "$(counter decode_errors)" -eq 0 ] || { echo "FAIL: decode errors" >&2; fail=1; }
[ "$(counter undigested)" -eq 0 ] || { echo "FAIL: blocks without digests" >&2; fail=1; }
if printf '%s\n' "$logs" | grep -q "usage in emergency band"; then
  echo "FAIL: usage reached the emergency band, so the run did not test paced eviction" >&2; fail=1
fi
[ "$(counter replays)" -ge 2 ] || { echo "FAIL: fewer than 2 replays (connect, reconnect)" >&2; fail=1; }
[ "$(counter replayed_batches)" -gt 0 ] || { echo "FAIL: nothing replayed" >&2; fail=1; }
[ "$(counter replay_failures)" -eq 0 ] || { echo "FAIL: replay failures" >&2; fail=1; }
[ "$(counter events_lost)" -eq 0 ] || { echo "FAIL: batches lost" >&2; fail=1; }
[ "$(counter cascaded)" -gt 0 ] || { echo "FAIL: no tail was deleted as one edge" >&2; fail=1; }
deleted=$(( $(counter deleted_root) + $(counter deleted_orphan) + $(counter deleted_internal) + $(counter deleted_leaf) + $(counter deleted_untracked) ))
[ "$(( $(counter deleted_internal) * 20 ))" -le "$deleted" ] \
  || { echo "FAIL: more than 5% of $deleted deletions cut a chain" >&2; fail=1; }
printf '%s\n' "$logs" | grep DELETION_END | sed 's/.*DELETION_END/DELETION_END/'

k exec fake-vllm -- python3 /app/fake_vllm.py check || { echo "FAIL: a live session or the preamble lost blocks" >&2; fail=1; }

if [ "${KEEP:-0}" != 1 ]; then
  kind delete cluster --name "$CLUSTER"
fi
[ $fail -eq 0 ] && echo PASS
exit $fail

#!/usr/bin/env bash
# kvreap on kind: the deploy/kserve kvreap component (radix eviction, KV events
# discovered through the headless Service) against a fake vLLM that writes a
# shared preamble, long-lived sessions and a stream of short ones to two tiers,
# a node-local emptyDir (LOCAL) and the shared PVC (REMOTE). The Deployment
# evicts the PVC and a kvreap sidecar in the fake vLLM's pod the emptyDir,
# each from its own tier's events. The fake vLLM starts first and its PUB
# stream is cut once mid-run through toxiproxy, so the Deployment only sees
# the preamble and both see the cut batches by replaying them.
# Passes when both prune repeatedly while every live session and the preamble
# stay whole on both tiers, the emptyDir stays under its sizeLimit, and no
# batch is lost.
#
#   tests/kind/e2e.sh            # KEEP=1 leaves the cluster up
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=${KIND_CLUSTER:-kvreap-e2e}
PRUNES=${PRUNES:-4}
TIMEOUT=${TIMEOUT:-600}
k() { kubectl --context "kind-$CLUSTER" -n kvreap-e2e "$@"; }

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
build_log=$(mktemp)
docker build --progress=plain --load -t kvreap:e2e . >"$build_log" 2>&1 || { tail -80 "$build_log"; exit 1; }
docker build -q --load -t fake-vllm:e2e tests/kind/fake-vllm
docker pull -q ghcr.io/shopify/toxiproxy:2.12.0
kind load docker-image --name "$CLUSTER" kvreap:e2e fake-vllm:e2e ghcr.io/shopify/toxiproxy:2.12.0
kubectl --context "kind-$CLUSTER" delete namespace kvreap-e2e openshift-monitoring --ignore-not-found --wait=true
kubectl --context "kind-$CLUSTER" apply --server-side -f \
  https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.80.0/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
kubectl --context "kind-$CLUSTER" delete pv kvreap-e2e-kv-cache --ignore-not-found --wait=true
docker exec "$CLUSTER-control-plane" sh -c \
  'mkdir -p /kv-e2e && { mountpoint -q /kv-e2e || mount -t tmpfs -o size=1g,mode=1777 tmpfs /kv-e2e; } && rm -rf /kv-e2e/*'
kubectl --context "kind-$CLUSTER" apply -k tests/kind/overlay
tls=$(mktemp -d)
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=e2e-model-kvreap-metrics \
  -keyout "$tls/tls.key" -out "$tls/tls.crt" 2>/dev/null
k create secret tls e2e-model-kvreap-metrics-tls --cert="$tls/tls.crt" --key="$tls/tls.key"
k scale deployment/e2e-model-kvreap --replicas=0
k wait --for=delete pod -l app.kubernetes.io/component=kvreap --timeout=60s
k apply -f tests/kind/overlay/fake-vllm.yaml
k wait --for=condition=Ready pod/fake-vllm --timeout=120s
sleep 5
k scale deployment/e2e-model-kvreap --replicas=1
k rollout status deployment/e2e-model-kvreap --timeout=120s

log() { k logs deployment/e2e-model-kvreap -c kvreap 2>/dev/null || true; }
local_log() { k logs fake-vllm -c kvreap 2>/dev/null || true; }
prunes() { "$1" | grep -c DELETION_END || true; }
toxiproxy() { k exec fake-vllm -c toxiproxy -- /toxiproxy-cli "$@" >/dev/null; }
cut=0
deadline=$((SECONDS + TIMEOUT))
until [ "$(prunes log)" -ge "$PRUNES" ] && [ "$(prunes local_log)" -ge "$PRUNES" ]; do
  if [ $SECONDS -ge $deadline ]; then
    log | tail -20
    local_log | tail -20
    echo "FAIL: fewer than $PRUNES prunes on a tier in ${TIMEOUT}s" >&2
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
# Per tier: the last chains status line, read before the fake vLLM's manifest so
# blocks_stored cannot run ahead of the blocks it says it wrote.
check_tier() {
  local tier=$1 locality=$2 logs=$3 chains
  chains=$(printf '%s\n' "$logs" | grep ' chains ' | tail -1)
  counter() { echo "$chains" | tr ' ' '\n' | sed -n "s/^$1=//p"; }
  bad() { echo "FAIL: $tier tier: $*" >&2; fail=1; }
  echo "$tier: $chains"
  [ "$(counter locality)" = "\"$locality\"" ] || bad "locality is $(counter locality), want $locality"
  [ "$(counter event_batches)" -gt 0 ] || bad "no event batches"
  [ "$(counter decode_errors)" -eq 0 ] || bad "decode errors"
  [ "$(counter undigested)" -eq 0 ] || bad "blocks without digests"
  if printf '%s\n' "$logs" | grep -q "usage in emergency band"; then
    bad "usage reached the emergency band, so the run did not test paced eviction"
  fi
  [ "$(counter replays)" -ge 2 ] || bad "fewer than 2 replays (connect, reconnect)"
  [ "$(counter replayed_batches)" -gt 0 ] || bad "nothing replayed"
  [ "$(counter replay_failures)" -eq 0 ] || bad "replay failures"
  [ "$(counter events_lost)" -eq 0 ] || bad "batches lost"
  [ "$(counter cascaded)" -gt 0 ] || bad "no tail was deleted as one edge"
  local deleted=$(( $(counter deleted_root) + $(counter deleted_orphan) + $(counter deleted_internal) + $(counter deleted_leaf) + $(counter deleted_untracked) ))
  [ "$(( $(counter deleted_internal) * 20 ))" -le "$deleted" ] \
    || bad "more than 5% of $deleted deletions cut a chain"
  stored=$(counter blocks_stored)
  printf '%s\n' "$logs" | grep DELETION_END | sed 's/.*DELETION_END/DELETION_END/' | tail -3
}
logs=$(log)
check_tier shared REMOTE "$logs"
shared_stored=$stored
check_tier local LOCAL "$(local_log)"
local_stored=$stored
checked=$(k exec fake-vllm -c main -- python3 /app/fake_vllm.py check) \
  || { echo "FAIL: a live session or the preamble lost blocks" >&2; fail=1; }
echo "$checked"
written=$(printf '%s\n' "$checked" | sed -n 's/.*"blocks_written_per_tier": \([0-9]*\).*/\1/p')
for s in "shared $shared_stored" "local $local_stored"; do
  set -- $s
  [ "$2" -le "$written" ] \
    || { echo "FAIL: $1 tier marked $2 blocks on disk, more than the $written written to it" >&2; fail=1; }
done
restarts=$(k get pod fake-vllm -o jsonpath='{.status.phase} {.status.containerStatuses[?(@.name=="kvreap")].restartCount}')
[ "$restarts" = "Running 0" ] \
  || { echo "FAIL: fake vLLM pod is '$restarts', want 'Running 0' (evicted past the emptyDir sizeLimit?)" >&2; fail=1; }

# Scrapes through kube-rbac-proxy from the namespace the metrics NetworkPolicy
# admits, as the PodMonitor does: the scraper token gets 200, no token 401, a
# token without the /metrics grant 403, and a pod in kvreap-e2e no connection.
url=https://e2e-model-kvreap-metrics.kvreap-e2e.svc:8443/metrics
scrape='import ssl, sys, urllib.request, urllib.error
req = urllib.request.Request(sys.argv[1])
if len(sys.argv) > 2:
    req.add_header("Authorization", "Bearer " + sys.argv[2])
try:
    body = urllib.request.urlopen(req, context=ssl._create_unverified_context(), timeout=5).read().decode()
    print(200); print(body)
except urllib.error.HTTPError as e:
    print(e.code)
except OSError as e:
    print("unreachable", e)'
mon() { kubectl --context "kind-$CLUSTER" -n openshift-monitoring "$@"; }
kubectl --context "kind-$CLUSTER" create namespace openshift-monitoring
mon run scraper --image=fake-vllm:e2e --image-pull-policy=Never --command -- sleep 3600 >/dev/null
mon wait --for=condition=Ready pod/scraper --timeout=60s >/dev/null
token=$(k get secret e2e-model-kvreap-scraper-token -o jsonpath='{.data.token}' | base64 -d)
stranger=$(k create token default)
scraped=$(mon exec scraper -- python3 -c "$scrape" "$url" "$token")
status() { printf '%s\n' "$1" | head -1; }
[ "$(status "$scraped")" = 200 ] || { echo "FAIL: scraper token got $(status "$scraped")" >&2; fail=1; }
got=$(status "$(mon exec scraper -- python3 -c "$scrape" "$url")")
[ "$got" = 401 ] || { echo "FAIL: no token got $got, want 401" >&2; fail=1; }
got=$(status "$(mon exec scraper -- python3 -c "$scrape" "$url" "$stranger")")
[ "$got" = 403 ] || { echo "FAIL: unprivileged token got $got, want 403" >&2; fail=1; }
got=$(status "$(k exec fake-vllm -c main -- python3 -c "$scrape" "$url" "$token")")
case "$got" in
  unreachable*) ;;
  *) echo "FAIL: a pod outside the monitoring namespaces got $got" >&2; fail=1 ;;
esac
series() { printf '%s\n' "$scraped" | awk -v s="$1" '$1 == s {print $2}'; }
for s in kvreap_files_deleted_total 'kvreap_chain_deletions_total{position="leaf"}' kvreap_kv_event_replays_total kvreap_prune_seconds_count; do
  v=$(series "$s")
  echo "metrics: $s = ${v:-missing}"
  awk -v v="${v:-0}" 'BEGIN { exit !(v > 0) }' || { echo "FAIL: /metrics has no $s > 0" >&2; fail=1; }
done

k exec fake-vllm -c main -- python3 /app/fake_vllm.py check || { echo "FAIL: a live session or the preamble lost blocks" >&2; fail=1; }

if [ "${KEEP:-0}" != 1 ]; then
  kind delete cluster --name "$CLUSTER"
fi
[ $fail -eq 0 ] && echo PASS
exit $fail

#!/usr/bin/env bash
# Runs the image the way the pvc-evictor Helm chart does and checks the drop-in contract:
# chart env vars parse, the chart's python3 probe works, cold blocks go, hot blocks stay,
# and SIGTERM exits 0.
set -euo pipefail

image="${1:?usage: $0 <image>}"
name="kvreap-smoke-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$name" --tmpfs /kv-cache:size=64m,exec \
  -e PVC_MOUNT_PATH=/kv-cache -e CACHE_DIRECTORY=kv/model-cache/models \
  -e CLEANUP_THRESHOLD=0.001 -e TARGET_THRESHOLD=0 \
  -e NUM_CRAWLER_PROCESSES=8 -e LOGGER_INTERVAL_SECONDS=0.5 \
  -e FILE_QUEUE_MAXSIZE=10000 -e FILE_QUEUE_MIN_SIZE=1000 -e DELETION_BATCH_SIZE=100 \
  -e DELETION_MAX_FILES_PER_SECOND=0 -e FILE_ACCESS_TIME_THRESHOLD_MINUTES=60 \
  -e ENABLE_DIR_CLEANUP=true -e DIR_CLEANUP_TTL_SECONDS=120 \
  -e DRY_RUN=false -e LOG_LEVEL=INFO -e LOG_FILE_PATH=/tmp/evictor_all_logs.txt \
  "$image" >/dev/null

docker exec "$name" python3 -c "import os; exit(0 if os.path.exists('/kv-cache') else 1)"

docker exec -i "$name" python3 - <<'PY'
import json, os, time
root = "/kv-cache/kv/model-cache/models"
base = os.path.join(root, "org-model_abcdef012345")
os.makedirs(base, exist_ok=True)
json.dump({"model_name": "org/model"}, open(os.path.join(base, "config.json"), "w"))
for i in range(120):
    h = f"{((i + 1) * 0x9E3779B97F4A7C15) & (2**64 - 1):016x}" * 4  # sha256-length, like vLLM 0.31
    d = os.path.join(root, "org-model_abcdef012345_r0", h[:3], h[3:5] + "_g0")
    os.makedirs(d, exist_ok=True)
    f = os.path.join(d, h + ".bin")
    open(f, "wb").write(b"x" * 4096)
    if i < 100:
        t = time.time() - 7200 - i
        os.utime(f, (t, t))
PY

count_bins() {
  docker exec "$name" python3 -c "
import os, time
bins = [os.path.join(d, f) for d, _, fs in os.walk('/kv-cache/kv/model-cache/models') for f in fs if f.endswith('.bin')]
hot = [b for b in bins if time.time() - os.stat(b).st_atime < 3600]
print(len(bins), len(hot))"
}

for _ in $(seq 1 120); do
  read -r total hot < <(count_bins)
  [ "$total" -eq 20 ] && break
  sleep 0.5
done
read -r total hot < <(count_bins)
[ "$total" -eq 20 ] && [ "$hot" -eq 20 ] || { echo "expected 20 hot blocks left, got total=$total hot=$hot"; docker logs "$name"; exit 1; }

docker exec "$name" grep -q "kvreap starting" /tmp/evictor_all_logs.txt

docker stop -t 10 "$name" >/dev/null
code="$(docker inspect -f '{{.State.ExitCode}}' "$name")"
[ "$code" -eq 0 ] || { echo "exit code $code after SIGTERM"; docker logs "$name"; exit 1; }
echo "container smoke test passed"

# kvbench

A/B/C benchmark of PVC evictors against vLLM with filesystem KV offloading,
run on a Kubernetes cluster with a VAST storage class.

Each run creates a fresh PVC, starts vLLM (`TieringOffloadingSpec` with an `fs`
tier on the PVC), deploys the evictor through the pvc-evictor Helm chart, and
drives a mix of unique prompts (churn) and repeated prefixes (hot reads).
Sampler sidecars record per-op NFS counters from `/proc/self/mountstats`,
statvfs usage and vLLM metrics every second. The PVC storage class is a clone
of `shared-vast` with `nosharecache`, so each pod's counters are its own.

```bash
uv sync
CHART=~/git/llm-d-kv-cache-evictor/kv_connectors/pvc_evictor/helm
uv run kvbench run --variant a --image quay.io/wseaton/pvc-evictor:a-f62b9a7 --chart $CHART --out results/a
uv run kvbench run --variant b --image quay.io/wseaton/pvc-evictor:b-2a03840 --chart $CHART --out results/b
uv run kvbench run --variant c --image quay.io/wseaton/kvreap:c-<sha> --chart $CHART --out results/c
uv run kvbench report results/a results/b results/c --out reports/<name>
```

`kvbench report` writes `README.md` with light/dark SVG charts and the full
metrics table; `kvbench analyze` prints the table only. Pass chart overrides
with `--evictor-values '{"config": {"deletionMaxFilesPerSecond": 200}}'`.

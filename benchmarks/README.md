# Benchmarks

All runs on CoreWeave (H200, vLLM 0.31 with the fs offload tier), produced by
the harness in [bench/](../bench/README.md). Each report lists the exact
images, settings and load.

## Current

| report | what it shows |
|---|---|
| [final-vast.md](final-vast.md) | Python `pvc_evictor` vs kvreap plain LRU vs kvreap chain-aware, Qwen2.5-72B TP=4 on a shared VAST PVC |
| [final-nvme.md](final-nvme.md) | kvreap plain LRU vs chain-aware on node-local NVMe through a hostPath |
| [hostpath-proof/](hostpath-proof/) | the Python evictor's own usage function on a hostPath: it never reaches the threshold |

## History

Earlier runs that led to the current design. They name `CHAIN_EVICTION`
policies (`observe`, `tail-first`, `subtree`, `radix`) that were later
removed: radix won every comparison and is now the only chain-aware
behavior, and `observe` was plain sampled LRU, which is what kvreap does
without `KV_EVENTS_ENDPOINTS`.

| report | what it shows |
|---|---|
| [sampling-vast.md](sampling-vast.md) | sampled LRU vs the Python evictor (and its quick fixes) on Qwen3-0.6B: metadata load and prune speed |
| [chain-spike.md](chain-spike.md) | first chain-aware policies on Qwen3-0.6B |
| [chain-spike-agent.md](chain-spike-agent.md) | radix vs plain LRU on an agentic Qwen3-32B load |
| [chain-spike-72b.md](chain-spike-72b.md) | radix vs plain LRU on Qwen2.5-72B, before the limiter and missing-file fixes |

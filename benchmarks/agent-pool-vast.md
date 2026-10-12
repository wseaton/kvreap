# Python pvc_evictor vs kvreap on VAST, Qwen2.5-72B TP=4

Synthetic agent load (nyann-bench `conversation_pool`). The final numbers are on replayed Claude Code sessions: [final-vast.md](final-vast.md).

| run | evictor image |
|---|---|
| pa | `quay.io/wseaton/pvc-evictor:a-f62b9a7` |
| gw | `quay.io/wseaton/kvreap:r-962281f` |
| vr7 | `quay.io/wseaton/kvreap:r-db6a55b` |

Final comparison on a shared VAST PVC: the llm-d Python `pvc_evictor` (upstream main), kvreap's plain sampled LRU (no KV events), and kvreap's chain-aware eviction (KV events, radix leaf edges). Qwen/Qwen2.5-72B-Instruct, TP=4, `--kv-cache-dtype fp8`, 4× H200 per run, fs tier on VAST through the striped tier plugin. nyann-bench `conversation_pool`: 32 sessions in rotation, 8 in flight, 2,048-token shared preamble, 8,000-token first turn, up to 30 turns of 500 tokens with 200-token replies. 40 minutes per run, 160Gi PVC, CPU tier 32 GiB, cleanup 85%, target 70%, hot threshold 2 minutes.

**pa** (Python) and **gw** (kvreap plain LRU, `r-962281f`) ran on 2026-10-10. **vr7** is the release candidate, `r-db6a55b`, run on 2026-10-11 with identical settings on the same cluster.

## Findings

| | pa: Python pvc_evictor | gw: kvreap plain LRU | vr7: kvreap chain-aware | vr7 vs pa |
|---|---|---|---|---|
| requests completed in 40 min | 2,748 | 2,734 | **4,471** | +63% |
| prefix tokens served from offload tiers | 35.5M of 48.8M | 34.4M of 48.3M | **74.6M of 78.9M** | 2.1× |
| returning-turn TTFT p50 | 645 ms | 643 ms | 645 ms | |
| returning-turn TTFT p90 | 6,295 ms | 4,408 ms | **993 ms** | 6.3× lower |
| returning-turn TTFT p99 | 18,104 ms | 11,032 ms | **2,187 ms** | 8.3× lower |
| first-turn TTFT p90 | 6,102 ms | 3,810 ms | **2,719 ms** | 2.2× lower |
| KV rewritten to VAST | 2.19 TB | 2.29 TB | **0.72 TB** | −67% |
| evictor NFS metadata ops/s (mean) | 14,765 | 879 | **2,801** | 5.3× fewer |
| deletions that cut a live chain (internal) | not tracked | 110,889 | **1** | |
| vLLM load failures / quota rejections | 266 / 0 | 851 / 0 | **0 / 0** | |
| max usage | 91.6% | 86.5% | 96.6% | |

- **Eviction choice is the difference.** The Python evictor deletes whatever its crawl reaches first, and plain LRU deletes oldest-written first; both cut live sessions at their oldest blocks, so every later turn of a cut session recomputes and rewrites. They tie on throughput. Chain-aware eviction deletes whole unshared session tails by last write and never a prefix two sessions share: it serves twice the prefix tokens from disk and completes 63% more requests.
- **Usage runs higher.** Below the pressure line (91%) kvreap holds back sessions written in the last 2 minutes so the sampler can find finished ones first; above it, it evicts in plain LRU order. Usage peaked at 96.6%, below the 97% emergency band and with no quota rejections, and spent 12.9% of the run above the cleanup threshold against 6.6% for the Python evictor.
- **Metadata cost.** Holding young sessions back costs extra sampling: 2,801 ops/s against 704 for an earlier radix build that deleted them more readily, still a fifth of the Python evictor's 14,765.
- **Python evictor on node-local NVMe** does not work at all: see `agent-pool-nvme.md` and `hostpath-proof/`.

## Setup

| setting | value |
|---|---|
| model | Qwen/Qwen2.5-72B-Instruct |
| vLLM image | docker.io/vllm/vllm-openai:v0.31.0 |
| PVC size | 160Gi |
| storage class | kvreap-bench-vast |
| load duration (s) | 2400 |
| offload block (tokens) | 16 |
| CPU tier (bytes) | 34359738368 |
| GPU KV blocks | 16384 |
| churn prompt length | 4096 |
| churn concurrency | 8 |
| hot prefixes | 64 |
| hot prefix length | 2048 |
| hot request rate (req/s) | 2.0 |

Time axes are seconds since the load generator started.

## PVC usage

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/usage-dark.svg"><img alt="PVC usage" src="agent-pool-vast/charts/usage.svg"></picture>

## Evictor metadata load

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/evictor-ops-dark.svg"><img alt="Evictor metadata load" src="agent-pool-vast/charts/evictor-ops.svg"></picture>

## Evictor ops by type

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/evictor-op-types-dark.svg"><img alt="Evictor ops by type" src="agent-pool-vast/charts/evictor-op-types.svg"></picture>

## vLLM LOOKUP latency

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/rtt-lookup-dark.svg"><img alt="vLLM LOOKUP latency" src="agent-pool-vast/charts/rtt-lookup.svg"></picture>

## vLLM CREATE latency

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/rtt-create-dark.svg"><img alt="vLLM CREATE latency" src="agent-pool-vast/charts/rtt-create.svg"></picture>

## vLLM WRITE latency

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/rtt-write-dark.svg"><img alt="vLLM WRITE latency" src="agent-pool-vast/charts/rtt-write.svg"></picture>

## vLLM READ latency

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/rtt-read-dark.svg"><img alt="vLLM READ latency" src="agent-pool-vast/charts/rtt-read.svg"></picture>

## Hot-prefix TTFT

<picture><source media="(prefers-color-scheme: dark)" srcset="agent-pool-vast/charts/ttft-dark.svg"><img alt="Hot-prefix TTFT" src="agent-pool-vast/charts/ttft.svg"></picture>

## All metrics

| metric | pa | gw | vr7 |
|---|---|---|---|
| load duration (s) | 2,407 | 2,405 | 2,404 |
| **evictor metadata ops/s (mean)** | 14,765.3 | 878.7 | 2,800.8 |
| evictor metadata ops (total) | 35,525,343 | 2,112,321 | 6,730,304 |
| evictor ACCESS/s (mean / p95 10s) | 251.3 / 669.3 | 103.7 / 704.9 | 140.1 / 810.0 |
| evictor CREATE/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor FSINFO/s (mean / p95 10s) | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor FSSTAT/s (mean / p95 10s) | 3.0 / 3.1 | 3.0 / 3.1 | 3.0 / 3.1 |
| evictor GETATTR/s (mean / p95 10s) | 13,905.5 / 20,643.8 | 496.8 / 3,247.9 | 2,197.8 / 11,483.2 |
| evictor LOOKUP/s (mean / p95 10s) | 136.6 / 744.9 | 2.4 / 1.5 | 99.9 / 836.1 |
| evictor NULL/s (mean / p95 10s) | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor PATHCONF/s (mean / p95 10s) | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor READ/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor READDIRPLUS/s (mean / p95 10s) | 218.9 / 785.4 | 151.4 / 987.5 | 202.4 / 1,537.5 |
| evictor REMOVE/s (mean / p95 10s) | 86.5 / 370.6 | 84.1 / 583.1 | 91.7 / 655.7 |
| evictor RENAME/s (mean / p95 10s) | - | - | 0.0 / 0.0 |
| evictor RMDIR/s (mean / p95 10s) | 163.4 / 438.9 | 37.3 / 263.7 | 65.8 / 360.0 |
| evictor SETATTR/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor WRITE/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 |
| vLLM LOOKUP RTT ms (all) | 0.67 | 0.57 | 0.68 |
| vLLM LOOKUP RTT ms (deleting / idle) | 0.73 / 0.63 | 0.49 / 0.59 | 0.65 / 0.73 |
| vLLM GETATTR RTT ms (all) | 1.40 | 1.31 | 1.57 |
| vLLM GETATTR RTT ms (deleting / idle) | 1.12 / 1.45 | 0.91 / 1.35 | 1.56 / 1.57 |
| vLLM CREATE RTT ms (all) | 1.62 | 1.67 | 1.87 |
| vLLM CREATE RTT ms (deleting / idle) | 1.57 / 1.64 | 1.55 / 1.72 | 1.73 / 2.01 |
| vLLM RENAME RTT ms (all) | 2.57 | 2.62 | 2.87 |
| vLLM RENAME RTT ms (deleting / idle) | 2.52 / 2.59 | 2.52 / 2.65 | 2.72 / 3.00 |
| vLLM WRITE RTT ms (all) | 5.53 | 5.58 | 6.36 |
| vLLM WRITE RTT ms (deleting / idle) | 5.28 / 5.61 | 5.20 / 5.71 | 5.80 / 6.90 |
| vLLM READ RTT ms (all) | 4.21 | 4.23 | 4.29 |
| vLLM READ RTT ms (deleting / idle) | 4.11 / 4.22 | 4.11 / 4.24 | 4.27 / 4.30 |
| vLLM LOOKUP client queue ms | 0.003 | 0.003 | 0.003 |
| vLLM GETATTR client queue ms | 0.005 | 0.005 | 0.006 |
| vLLM CREATE client queue ms | 0.003 | 0.003 | 0.003 |
| vLLM RENAME client queue ms | 0.012 | 0.013 | 0.013 |
| vLLM WRITE client queue ms | 0.186 | 0.202 | 0.195 |
| vLLM READ client queue ms | 0.016 | 0.016 | 0.016 |
| vLLM FS read s/GiB | 3.12 | 3.10 | 3.09 |
| vLLM FS write s/GiB | 2.22 | 1.82 | 7.65 |
| vLLM metadata ops/s | 2,871.9 | 2,577.8 | 3,867.6 |
| prune cycles | 12 | 13 | 8 |
| prune seconds (median) | 40.6 | 20.0 | 75.4 |
| max usage % | 91.6 | 86.5 | 96.6 |
| % time >= cleanup threshold | 6.6 | 5.9 | 12.9 |
| hot ttft p50 ms | - | - | - |
| hot ttft p90 ms | - | - | - |
| hot ttft p99 ms | - | - | - |
| agent requests | 2,748 | 2,734 | 4,471 |
| agent failed | 0 | 0 | 0 |
| agent first ttft p50 ms | 1,847.3 | 1,560.4 | 1,420.3 |
| agent first ttft p90 ms | 6,101.9 | 3,810.1 | 2,719.0 |
| agent later ttft p50 ms | 645.2 | 642.5 | 645.4 |
| agent later ttft p90 ms | 6,295.1 | 4,407.7 | 993.1 |
| agent later ttft p99 ms | 18,103.7 | 11,031.7 | 2,186.7 |
| agent prompt tok per s | - | - | - |
| hot requests (failed) | - (-) | - (-) | - (-) |
| churn input tok/s | - | - | - |
| vLLM log: quota_exceeded | 0 | 0 | 0 |
| vLLM log: store_failed | 0 | 0 | 0 |
| vLLM log: load_failed | 266 | 851 | 0 |
| chain deletions: heads (root + orphan) | - | 63,966 | - |
| chain deletions: root | - | 132 | 0 |
| chain deletions: orphan (parent gone) | - | 63,834 | 1 |
| chain deletions: internal | - | 110,889 | 1 |
| chain deletions: leaf | - | 28,767 | 220,424 |
| chain deletions: untracked | - | 0 | 0 |
| chain deferrals | - | 0 | - |
| chain deletions: cascaded below a deleted root | - | 0 | 220,286 |
| blocks announced without a digest | - | 0 | 0 |
| leaf edges skipped as too young | - | 0 | 131,331 |
| young leaf edges deleted under pressure | - | 0 | - |
| young leaf edges deleted in the emergency band | - | - | - |
| KV event batches received | - | 37,201 | 59,618 |
| KV event decode errors | - | 0 | 0 |
| `vllm:external_prefix_cache_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 35,547,984 | 34,426,208 | 74,594,688 |
| `vllm:external_prefix_cache_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 48,755,956 | 48,338,373 | 78,936,562 |
| `vllm:kv_offload_load_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 5,824,181,698,560 | 5,640,389,918,720 | 12,221,593,681,920 |
| `vllm:kv_offload_load_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 218 | 214 | 447 |
| `vllm:kv_offload_store_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 2,184,225,751,040 | 2,294,174,187,520 | 721,386,209,280 |
| `vllm:kv_offload_store_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 82 | 102 | 24 |
| `vllm:kv_offload_tiering_chunk_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="0:primary"}` Δ | - | - | 125 |
| `vllm:kv_offload_tiering_chunk_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 2,244,351 | 2,176,361 | 4,662,168 |
| `vllm:kv_offload_tiering_chunk_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="0:primary"}` Δ | 3,045,919 | 3,020,925 | 4,931,370 |
| `vllm:kv_offload_tiering_chunk_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 2,247,115 | 2,179,102 | 4,666,648 |
| `vllm:kv_offload_tiering_promotion_job_failures_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 68 | 126 | - |
| `vllm:kv_offload_tiering_read_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 5,824,181,698,560 | 5,640,389,918,720 | 12,221,593,681,920 |
| `vllm:kv_offload_tiering_read_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 16,923 | 16,303 | 35,206 |
| `vllm:kv_offload_tiering_write_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 2,185,567,928,320 | 2,294,174,187,520 | 721,386,209,280 |
| `vllm:kv_offload_tiering_write_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 4,510 | 3,897 | 5,137 |
| `vllm:kv_offload_total_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="CPU_to_GPU"}` Δ | 5,824,181,698,560 | 5,640,389,918,720 | 12,221,593,681,920 |
| `vllm:kv_offload_total_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="GPU_to_CPU"}` Δ | 2,184,225,751,040 | 2,294,174,187,520 | 721,386,209,280 |
| `vllm:kv_offload_total_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="CPU_to_GPU"}` Δ | 218 | 214 | 447 |
| `vllm:kv_offload_total_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="GPU_to_CPU"}` Δ | 82 | 102 | 24 |
| `vllm:prefix_cache_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 4,435,680 | 4,442,768 | 7,117,824 |
| `vllm:prefix_cache_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 53,191,636 | 52,781,141 | 86,054,386 |

# Final: Python pvc_evictor vs kvreap on VAST, replayed Claude Code sessions, Qwen2.5-72B TP=4

| run | evictor image |
|---|---|
| wpa | `quay.io/wseaton/pvc-evictor:a-f62b9a7` |
| wvp | `quay.io/wseaton/kvreap:r-37c777a` |
| wvn | `quay.io/wseaton/kvreap:r-e0e67c9` |
| wp320 | `quay.io/wseaton/pvc-evictor:a-f62b9a7` |
| wvn320 | `quay.io/wseaton/kvreap:r-e0e67c9` |

Replayed Claude Code sessions (SemiAnalysis Weka traces, AIPerf; see [Workload](#workload)) against Qwen/Qwen2.5-72B-Instruct, TP=4, fp8 KV, YaRN to 131,072 tokens, 4× H200, fs tier on a shared VAST PVC through the striped tier plugin, CPU tier 32 GiB, cleanup 85%, target 70%, hot threshold 2 minutes. 40 minutes of load per run, at two cache sizes. vLLM publishes KV events in every run; only the chain-aware runs subscribe.

| run | evictor | cache | load start (UTC) |
|---|---|---|---|
| wpa | Python `pvc_evictor` (llm-d-kv-cache main) | 160 GiB | 2026-10-12 00:06 |
| wvp | kvreap plain LRU (`KV_EVENTS_ENDPOINTS` empty), `r-37c777a` | 160 GiB | 2026-10-12 01:41 |
| wvn | kvreap chain-aware, `r-e0e67c9` | 160 GiB | 2026-10-12 17:56 |
| wp320 | Python `pvc_evictor` | 320 GiB | 2026-10-12 12:11 |
| wvn320 | kvreap chain-aware, `r-e0e67c9` | 320 GiB | 2026-10-12 18:43 |

All runs used vLLM node gf48cf2, one after another. `r-e0e67c9` is the release candidate: radix victims come from the chain index's global last-use order. Plain LRU does not use the chain index, so the earlier image measures it equally.

## Findings

| | wpa: Python 160 | wvp: plain 160 | wvn: chain 160 | wp320: Python 320 | wvn320: chain 320 |
|---|---|---|---|---|---|
| requests completed in 40 min | 618 | 632 | **709** (+15%) | 699 | **851** (+22%) |
| prompt tokens served from the fs tier | 2.9M of 15.7M | 2.7M of 15.1M | **8.0M of 18.2M** | 7.0M of 18.8M | **17.3M of 26.2M** |
| first-request TTFT p90 | 33.9 s | 20.2 s | **14.0 s** | 20.3 s | **16.8 s** |
| later main-agent TTFT p50 | 7.0 s | 6.4 s | **4.1 s** | **4.1 s** | 4.9 s |
| later main-agent TTFT p90 / p99 | 37.2 / 75.2 s | 39.3 / 83.1 s | 38.9 / 94.2 s | **31.1 / 70.4 s** | 47.8 / 72.7 s |
| failed fs reads (deleted between lookup and read) | 302 | **145** | 1,841 | 413 | **0** |
| deletions that cut a live chain (internal) | not tracked | not tracked | **0** | not tracked | **0** |
| KV written to VAST | 3.47 TB | 3.43 TB | 3.36 TB | 3.44 TB | **3.02 TB** |
| evictor NFS metadata ops/s (mean) | 10,225 | 1,687 | **975** | 14,397 | **420** |
| max usage / quota rejections | 90.1% / 0 | 90.4% / 0 | 91.2% / 0 | 87.6% / 0 | 87.8% / 0 |

- **Size the cache to hold the active sessions.** At 320 GiB, where finished sessions and subagent chains fit beside the live ones, chain-aware eviction completes 22% more requests than the Python evictor, serves 2.5× the prompt tokens from VAST, writes 12% less KV, fails no reads, cuts no chain, and matches the Python evictor's tail (p99 72.7 s against 70.4 s) at 3% of its metadata load.
- **Undersized, LRU thrashes.** At 160 GiB the live sessions do not fit, so every evictor deletes live data. Exact last-use order deletes the session that has been idle longest, which on this load is often the next one to come back: 1,841 failed reads against 302 for the Python evictor, whose crawl order is closer to random. It still completes 15% more requests and serves 2.7× the tokens from disk.
- **The gain is smaller than on a synthetic agent load** ([agent-pool-vast.md](agent-pool-vast.md): +63% requests). These sessions are long, and the GPU prefix cache serves about half of all prompt tokens in every run.
- **Tail latency is queueing.** The load grows as sessions grow, and vLLM has requests waiting most of the second half of every run; a run that completes more requests keeps the queue as long. Compare requests completed and tokens served, not tail TTFT.
- **kvreap's plain LRU ties the Python evictor**, so the gain is the eviction choice. The previous release candidate (`r-75c1b4a`, victims from a sampled pool) got 704 and 827 requests with 556 and 135 chain cuts.
- **Context overflow.** AIPerf rebuilds some subagent prompts past the 131,072-token context; the same requests fail in every run that reaches them (21-46 per run).

n = 1 per configuration, back to back on one node with identical settings. The synthetic agent-load results are in [agent-pool-vast.md](agent-pool-vast.md).

## Workload

Replayed Claude Code sessions from the SemiAnalysis Weka trace corpus (`semianalysisai/cc-traces-weka-062126-256k`, revision `8fecd2f`: 393 sessions, 68,266 model requests, subagents included) with NVIDIA AIPerf 0.13.0, image `quay.io/wseaton/kvbench-aiperf:0.13.0-w062126` built from `bench/aiperf-image/`. Each trace records, per request, the input length, output length, think time and the request's 64-token KV block hashes. AIPerf rebuilds every prompt from those hashes and replays the session's recorded history (not the live responses), so every run sends the same token sequences and the prefix sharing of the real sessions, inside a session and between a session and its subagents, is reproduced exactly. Traces share nothing with each other.

| choice | value | why |
|---|---|---|
| model | Qwen2.5-72B-Instruct, TP=4, fp8 KV, YaRN ×4 to 131,072 tokens | same model and KV size (164 KB/token) as the earlier 72B runs; Claude Code contexts run past 100k tokens. vLLM 0.31 needs `max_position_embeddings` raised in `--hf-overrides` as well as `rope_parameters`. |
| corpus | the 256k-capped variant | the full-context corpus has requests up to 1M tokens |
| dropped requests | input over 120,000 tokens | leaves room in 131,072 for the output and for AIPerf's rebuilt history, which can run several percent past the recorded input length. This drops 36% of the corpus's requests: 58% of main-agent turns (Claude Code's main context usually runs past 120k before it compacts) and 21% of subagent requests. A main session replays until its context passes 120k tokens, then skips to its next compaction and continues from there. |
| sessions in flight | 6 | AIPerf concurrency mode: 6 traces at a time, each with its subagents; a finished trace is replaced by the next one in corpus order (96 loaded, about 20 started per run) |
| think time | the recorded per-request think time, capped at 60 s | keeps the gaps between tool calls and the user's short pauses; drops the hour-long breaks |
| output | the recorded length, `ignore_eos`, temperature 0; main-agent outputs capped at 1,024 tokens | identical decode work in every run |
| GPU KV cache | 40,960 blocks (655k tokens) | 6 sessions plus their subagents keep 0.8-1.4M tokens of context live (requests within the last 90 s), more than the GPU holds |
| CPU tier | 32 GiB (210k tokens) | as before |
| fs tier | 160 GiB (1.05M tokens) and 320 GiB (2.1M tokens) | 160 GiB is about the live working set, so every evictor has to cut live sessions; 320 GiB also holds the finished sessions and subagent chains |

Pilots (not reported): 16 sessions queued requests for minutes (TTFT p50 20 s); 8 with a 1M-token GPU cache kept more context live than the fs tier holds and the evictor rewrote live prefixes at 2.6 GB/s; 4 sessions barely touched the fs tier.

## Setup

| setting | value |
|---|---|
| model | Qwen/Qwen2.5-72B-Instruct |
| vLLM image | docker.io/vllm/vllm-openai:v0.31.0 |
| max model length | 131072 |
| HF config overrides | {'max_position_embeddings': 131072, 'rope_parameters': {'rope_type': 'yarn', 'factor': 4.0, 'original_max_position_embeddings': 32768, 'rope_theta': 1000000.0}} |
| cache size (PVC, or budget on the host path) | 160Gi / 320Gi |
| storage class | kvreap-bench-vast |
| load duration (s) | 2400 |
| offload block (tokens) | 16 |
| CPU tier (bytes) | 34359738368 |
| GPU KV blocks | 40960 |
| workload | weka |
| AIPerf image | quay.io/wseaton/kvbench-aiperf:0.13.0-w062126 |
| Weka dataset | semianalysis_cc_traces_weka_062126_256k |
| traces loaded | 96 |
| concurrent sessions | 6 |
| max input tokens (longer requests dropped) | 120000 |
| max output tokens (root sessions) | 1024 |
| think-time cap (s) | 60.0 |
| random seed | 42 |

Time axes are seconds since the load generator started.

## PVC usage

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/usage-dark.svg"><img alt="PVC usage" src="final-vast/charts/usage.svg"></picture>

## Evictor metadata load

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/evictor-ops-dark.svg"><img alt="Evictor metadata load" src="final-vast/charts/evictor-ops.svg"></picture>

## Evictor ops by type

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/evictor-op-types-dark.svg"><img alt="Evictor ops by type" src="final-vast/charts/evictor-op-types.svg"></picture>

## vLLM LOOKUP latency

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/rtt-lookup-dark.svg"><img alt="vLLM LOOKUP latency" src="final-vast/charts/rtt-lookup.svg"></picture>

## vLLM CREATE latency

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/rtt-create-dark.svg"><img alt="vLLM CREATE latency" src="final-vast/charts/rtt-create.svg"></picture>

## vLLM WRITE latency

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/rtt-write-dark.svg"><img alt="vLLM WRITE latency" src="final-vast/charts/rtt-write.svg"></picture>

## vLLM READ latency

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/rtt-read-dark.svg"><img alt="vLLM READ latency" src="final-vast/charts/rtt-read.svg"></picture>

## TTFT of requests that can reuse a prefix

<picture><source media="(prefers-color-scheme: dark)" srcset="final-vast/charts/ttft-dark.svg"><img alt="TTFT of requests that can reuse a prefix" src="final-vast/charts/ttft.svg"></picture>

## All metrics

| metric | wpa | wvp | wvn | wp320 | wvn320 |
|---|---|---|---|---|---|
| load duration (s) | 2,436 | 2,442 | 2,435 | 2,445 | 2,435 |
| **evictor metadata ops/s (mean)** | 10,224.8 | 1,686.7 | 974.5 | 14,396.7 | 419.6 |
| evictor metadata ops (total) | 24,897,367 | 4,117,288 | 2,371,962 | 35,185,639 | 1,021,400 |
| evictor ACCESS/s (mean / p95 10s) | 379.8 / 926.3 | 200.5 / 780.9 | 241.3 / 1,332.7 | 291.0 / 868.7 | 111.2 / 683.5 |
| evictor CREATE/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 | - | 0.0 / 0.0 |
| evictor FSINFO/s (mean / p95 10s) | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor FSSTAT/s (mean / p95 10s) | 3.0 / 3.1 | 3.0 / 3.1 | 3.0 / 3.1 | 3.0 / 3.1 | 3.0 / 3.1 |
| evictor GETATTR/s (mean / p95 10s) | 8,875.3 / 19,071.5 | 944.4 / 3,943.5 | 92.6 / 433.6 | 13,481.0 / 22,268.5 | 17.4 / 64.5 |
| evictor LOOKUP/s (mean / p95 10s) | 144.8 / 506.5 | 9.1 / 62.2 | 403.8 / 2,269.7 | 73.6 / 250.9 | 189.1 / 930.1 |
| evictor NULL/s (mean / p95 10s) | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor PATHCONF/s (mean / p95 10s) | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 | 0.0 / 0.0 |
| evictor READ/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 | - | 0.0 / 0.0 |
| evictor READDIRPLUS/s (mean / p95 10s) | 365.0 / 1,067.1 | 288.2 / 1,161.5 | 18.5 / 84.3 | 260.1 / 717.8 | 2.4 / 18.4 |
| evictor REMOVE/s (mean / p95 10s) | 166.2 / 385.2 | 165.0 / 670.4 | 203.6 / 1,143.8 | 104.6 / 378.0 | 95.6 / 470.4 |
| evictor RENAME/s (mean / p95 10s) | - | - | - | - | 0.0 / 0.0 |
| evictor RMDIR/s (mean / p95 10s) | 290.7 / 422.6 | 76.6 / 346.7 | 11.7 / 59.4 | 183.4 / 415.3 | 0.9 / 3.8 |
| evictor SETATTR/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 | - | 0.0 / 0.0 |
| evictor WRITE/s (mean / p95 10s) | - | 0.0 / 0.0 | 0.0 / 0.0 | - | 0.0 / 0.0 |
| vLLM LOOKUP RTT ms (all) | 0.62 | 0.57 | 0.52 | 0.53 | 0.54 |
| vLLM LOOKUP RTT ms (deleting / idle) | 0.63 / 0.61 | 0.58 / 0.55 | 0.54 / 0.51 | 0.54 / 0.53 | 0.46 / 0.55 |
| vLLM GETATTR RTT ms (all) | 0.73 | 0.72 | 1.44 | 1.39 | 2.05 |
| vLLM GETATTR RTT ms (deleting / idle) | 0.67 / 0.79 | 0.68 / 0.76 | 1.16 / 1.61 | 0.72 / 1.54 | 1.54 / 2.08 |
| vLLM CREATE RTT ms (all) | 1.59 | 1.65 | 1.60 | 1.55 | 1.64 |
| vLLM CREATE RTT ms (deleting / idle) | 1.58 / 1.60 | 1.65 / 1.64 | 1.61 / 1.59 | 1.55 / 1.55 | 1.55 / 1.65 |
| vLLM RENAME RTT ms (all) | 2.61 | 2.65 | 2.56 | 2.55 | 2.66 |
| vLLM RENAME RTT ms (deleting / idle) | 2.60 / 2.62 | 2.65 / 2.64 | 2.55 / 2.56 | 2.55 / 2.55 | 2.54 / 2.67 |
| vLLM WRITE RTT ms (all) | 5.17 | 5.19 | 5.07 | 5.18 | 5.48 |
| vLLM WRITE RTT ms (deleting / idle) | 5.16 / 5.18 | 5.18 / 5.19 | 5.03 / 5.10 | 5.13 / 5.21 | 5.15 / 5.51 |
| vLLM READ RTT ms (all) | 3.89 | 4.03 | 4.53 | 4.59 | 4.86 |
| vLLM READ RTT ms (deleting / idle) | 3.72 / 4.01 | 4.05 / 4.02 | 4.36 / 4.60 | 3.89 / 4.65 | 4.68 / 4.86 |
| vLLM LOOKUP client queue ms | 0.003 | 0.003 | 0.003 | 0.004 | 0.004 |
| vLLM GETATTR client queue ms | 0.004 | 0.005 | 0.005 | 0.005 | 0.006 |
| vLLM CREATE client queue ms | 0.003 | 0.003 | 0.004 | 0.003 | 0.004 |
| vLLM RENAME client queue ms | 0.012 | 0.013 | 0.014 | 0.014 | 0.014 |
| vLLM WRITE client queue ms | 0.158 | 0.171 | 0.169 | 0.169 | 0.177 |
| vLLM READ client queue ms | 0.015 | 0.016 | 0.016 | 0.015 | 0.015 |
| vLLM FS read s/GiB | 3.59 | 3.55 | 3.74 | 3.80 | 3.88 |
| vLLM FS write s/GiB | 2.31 | 2.16 | 2.42 | 1.79 | 1.90 |
| vLLM metadata ops/s | 2,475.6 | 2,349.7 | 3,061.0 | 2,380.5 | 3,322.8 |
| prune cycles | 16 | 16 | 22 | 6 | 9 |
| prune seconds (median) | 65.4 | 60.1 | 25.3 | 120.7 | 10.0 |
| max usage % | 90.1 | 90.4 | 91.2 | 87.6 | 87.8 |
| % time >= cleanup threshold | 13.2 | 7.4 | 9.0 | 4.5 | 3.7 |
| hot ttft p50 ms | - | - | - | - | - |
| hot ttft p90 ms | - | - | - | - | - |
| hot ttft p99 ms | - | - | - | - | - |
| weka requests | 618 | 632 | 709 | 699 | 851 |
| weka failed | 21 | 21 | 21 | 39 | 46 |
| weka cancelled | 0 | 0 | 0 | 0 | 0 |
| weka sessions | 19 | 19 | 19 | 19 | 21 |
| weka ttft p50 ms | 6,680.5 | 6,911.0 | 4,623.4 | 5,091.0 | 5,434.2 |
| weka ttft p90 ms | 50,861.0 | 50,784.6 | 57,492.6 | 52,903.6 | 54,643.5 |
| weka ttft p99 ms | 96,127.8 | 99,035.7 | 116,733.1 | 85,655.0 | 90,630.5 |
| weka first ttft p50 ms | 4,968.9 | 7,883.2 | 4,217.0 | 6,723.9 | 4,530.4 |
| weka first ttft p90 ms | 33,926.3 | 20,150.4 | 14,013.3 | 20,278.2 | 16,759.9 |
| weka later ttft p50 ms | 6,953.9 | 6,417.3 | 4,128.0 | 4,073.5 | 4,894.9 |
| weka later ttft p90 ms | 37,244.8 | 39,284.8 | 38,934.5 | 31,146.0 | 47,808.7 |
| weka later ttft p99 ms | 75,187.2 | 83,073.8 | 94,174.0 | 70,356.0 | 72,698.3 |
| weka subagent ttft p50 ms | 6,449.7 | 7,274.4 | 5,026.5 | 5,647.4 | 5,890.4 |
| weka subagent ttft p90 ms | 56,227.2 | 53,676.7 | 64,254.1 | 58,670.5 | 58,962.0 |
| weka subagent ttft p99 ms | 100,774.0 | 100,605.9 | 124,188.3 | 86,369.4 | 95,932.1 |
| weka prompt tok per s | 14,420.9 | 14,829.1 | 17,142.7 | 16,732.8 | 20,847.1 |
| weka output tok per s | 135.0 | 138.5 | 158.0 | 153.7 | 187.4 |
| hot requests (failed) | - (-) | - (-) | - (-) | - (-) | - (-) |
| churn input tok/s | - | - | - | - | - |
| vLLM log: quota_exceeded | 0 | 0 | 0 | 0 | 0 |
| vLLM log: store_failed | 0 | 0 | 0 | 0 | 0 |
| vLLM log: load_failed | 302 | 145 | 1,841 | 413 | 0 |
| chain deletions: heads (root + orphan) | - | - | - | - | - |
| chain deletions: root | - | - | 3,915 | - | 0 |
| chain deletions: orphan (parent gone) | - | - | 0 | - | 0 |
| chain deletions: internal | - | - | 0 | - | 0 |
| chain deletions: leaf | - | - | 491,615 | - | 232,802 |
| chain deletions: untracked | - | - | 0 | - | 0 |
| chain deferrals | - | - | - | - | - |
| chain deletions: cascaded below a deleted root | - | - | 490,904 | - | 232,655 |
| blocks announced without a digest | - | - | 0 | - | 0 |
| leaf edges skipped as too young | - | - | - | - | - |
| young leaf edges deleted under pressure | - | - | - | - | - |
| young leaf edges deleted in the emergency band | - | - | - | - | - |
| KV event batches received | - | - | 21,459 | - | 25,123 |
| KV event decode errors | - | - | 0 | - | 0 |
| `vllm:external_prefix_cache_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 2,931,296 | 2,652,544 | 7,958,624 | 6,977,184 | 17,294,784 |
| `vllm:external_prefix_cache_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 15,748,119 | 15,085,913 | 18,226,110 | 18,830,849 | 26,227,067 |
| `vllm:kv_offload_allocation_failure_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 80 | 20 | 40 | 91 | 401 |
| `vllm:kv_offload_load_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 482,790,604,800 | 441,429,524,480 | 1,306,766,868,480 | 1,162,336,010,240 | 2,844,359,393,280 |
| `vllm:kv_offload_load_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 16 | 15 | 42 | 38 | 92 |
| `vllm:kv_offload_store_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 3,469,368,360,960 | 3,427,150,069,760 | 3,360,575,979,520 | 3,441,413,324,800 | 3,023,707,832,320 |
| `vllm:kv_offload_store_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 156 | 149 | 157 | 148 | 145 |
| `vllm:kv_offload_tiering_cascade_job_failures_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 2 | 1 | - | - | - |
| `vllm:kv_offload_tiering_chunk_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="0:primary"}` Δ | 145,609 | 105,245 | 84,318 | 119,255 | 114,926 |
| `vllm:kv_offload_tiering_chunk_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 137,994 | 160,180 | 520,578 | 451,335 | 1,159,407 |
| `vllm:kv_offload_tiering_chunk_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="0:primary"}` Δ | 1,010,673 | 967,439 | 1,241,029 | 1,236,148 | 1,744,133 |
| `vllm:kv_offload_tiering_chunk_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 138,799 | 160,965 | 521,323 | 452,108 | 1,160,210 |
| `vllm:kv_offload_tiering_promotion_allocation_failures_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | - | 16 | 982 | 2,409 | 8,752 |
| `vllm:kv_offload_tiering_promotion_job_failures_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 80 | 38 | 49 | 39 | - |
| `vllm:kv_offload_tiering_read_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 390,500,188,160 | 485,676,810,240 | 2,985,010,135,040 | 2,332,137,881,600 | 8,480,935,116,800 |
| `vllm:kv_offload_tiering_read_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 1,305 | 1,604 | 10,390 | 8,261 | 30,631 |
| `vllm:kv_offload_tiering_write_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 3,470,713,159,680 | 3,431,179,223,040 | 3,365,939,445,760 | 3,446,774,169,600 | 3,027,734,364,160 |
| `vllm:kv_offload_tiering_write_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",tier="1:StripedFileSystemTierManager"}` Δ | 7,473 | 6,918 | 7,575 | 5,733 | 5,363 |
| `vllm:kv_offload_total_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="CPU_to_GPU"}` Δ | 482,790,604,800 | 441,429,524,480 | 1,306,766,868,480 | 1,162,336,010,240 | 2,844,359,393,280 |
| `vllm:kv_offload_total_bytes_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="GPU_to_CPU"}` Δ | 3,469,368,360,960 | 3,427,150,069,760 | 3,360,575,979,520 | 3,441,413,324,800 | 3,023,707,832,320 |
| `vllm:kv_offload_total_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="CPU_to_GPU"}` Δ | 16 | 15 | 42 | 38 | 92 |
| `vllm:kv_offload_total_time_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct",transfer_type="GPU_to_CPU"}` Δ | 156 | 149 | 157 | 148 | 145 |
| `vllm:prefix_cache_hits_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 20,011,376 | 21,379,696 | 23,713,632 | 22,460,096 | 24,939,728 |
| `vllm:prefix_cache_queries_total{engine="0",model_name="Qwen/Qwen2.5-72B-Instruct"}` Δ | 35,759,495 | 36,465,609 | 41,939,742 | 41,290,945 | 51,166,795 |

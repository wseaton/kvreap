# kvreap with KServe LLMInferenceService

Chain-aware eviction for an `LLMInferenceService` whose `kvCacheOffloading`
puts a filesystem tier on a shared (RWX) PVC. kvreap deletes whole unshared
conversation tails (`CHAIN_EVICTION=radix`) instead of cutting prefix chains in
the middle; see `benchmarks/chain-spike-agent.md` for the measured effect.

| path | what |
|---|---|
| `preset/` | `LLMInferenceServiceConfig` `kv-offloading-kvreap`, added to the service's `spec.baseRefs` |
| `kvreap/` | kustomize Component: headless Service over the workload pods, the kvreap Deployment on the PVC, a NetworkPolicy for the KV events port |
| `example/` | an overlay that includes both for a service named `qwen3-32b` |

```bash
kubectl kustomize deploy/kserve/example
```

## What the service needs

- `spec.kvCacheOffloading.secondary[0].fileSystem.pvc.ref` naming an RWX PVC.
  kvreap mounts the same PVC; set `data.pvc` in your `kvreap-target` ConfigMap
  to that name. If the ref sets a `path`, set kvreap's `CACHE_DIRECTORY` to it.
- kvreap runs as uid/gid 1000 with `fsGroup: 1000`; change these to the uid
  vLLM writes the cache with, or kvreap cannot unlink in its directories.
- `spec.baseRefs: [{name: kv-offloading-kvreap}]`. The preset makes vLLM bind
  its KV events PUB socket on 5557 (`tcp://*:5557`) and replay socket on 5558,
  send full block hashes
  (`VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES=0`), and turn on the fs tier's own
  `STORAGE` events. It replaces the `--kv-transfer-config` KServe renders from
  `kvCacheOffloading`, because the rendered one has no `enable_kv_events`;
  keep `cpu_bytes_to_use` in line with `kvCacheOffloading.cpu`. With a KServe
  that has `secondary[].fileSystem.kvEvents` (wseaton/kserve branch
  `llmisvc-fs-tier-kv-events`), set `kvEvents: true` on the tier and drop the
  `--kv-transfer-config` from the preset.
- The EPP in discovery mode, so it and kvreap both connect to each vLLM pod:
  in the precise prefix cache producer's `kvEventsConfig`, set
  `discoverPods: true` and `podDiscoveryConfig.socketPort: 5557`, and drop the
  `zmqEndpoint` the EPP would otherwise bind. The EPP's Go adapter takes the
  last 8 bytes of a byte hash, the same value vLLM sends as an int, so the
  scorer is unaffected by full hashes.

## Events endpoint

`KV_EVENTS_ENDPOINTS` names the headless Service `<llmisvc>-kv-events`. kvreap
resolves it every 15 s and keeps one SUB socket per pod address, so pods that
restart or scale are picked up without a restart. Comma-separate entries to
follow several services.

Each SUB socket tracks the publisher's sequence numbers the way llm-d's router
does. On connect, on a gap, on joining mid-stream and when vLLM restarts
(sequence goes backwards), kvreap asks the pod's replay socket
(`KV_EVENTS_REPLAY_PORT`, 5558) for everything it has not applied; vLLM keeps
the last 10,000 batches. A failed replay backs off 30 s. The `chains` status
line counts `gaps`, `resets`, `replays`, `replay_failures` and `events_lost`
(batches gone from the buffer before kvreap could fetch them).

## Security

BlockStored events carry prompt token ids. kvreap implements no ZMQ
authentication (NULL mechanism, no crypto in the binary); run both sides in
the mesh so the sidecars provide mTLS, keep the port named `tcp-kv-events` so
the mesh treats it as opaque TCP, and apply the NetworkPolicy, which allows
5557 and 5558 only from kvreap and the EPP. The policy selects the workload pods for
Ingress, so it lists 8000 as open to all; add any other port your workload
serves before applying it.

## Optional: faster fs tier reads

vLLM 0.31's fs tier reads each promotion on one thread. Until vllm#58404 lands,
`bench/vllm_plugins/striped_fs.py` is an out-of-tree tier that splits loads
across the I/O pool (returning-turn TTFT p50 5.7 s to 0.86 s on Qwen3-32B over
VAST); load it with `"type": "StripedFileSystemTierManager",
"module_path": "striped_fs"` in the preset's tier and the file on vLLM's
`PYTHONPATH`.

# kvreap with KServe LLMInferenceService

Chain-aware eviction for an `LLMInferenceService` whose `kvCacheOffloading`
puts a filesystem tier on a shared (RWX) PVC. kvreap deletes whole unshared
conversation tails instead of cutting prefix chains in the middle; see
`benchmarks/final-vast.md` and `benchmarks/final-nvme.md` for the measured effect.

| path | what |
|---|---|
| `preset/` | `LLMInferenceServiceConfig` `kv-offloading-kvreap`, added to the service's `spec.baseRefs` |
| `kvreap/` | kustomize Component: headless Service over the workload pods and the kvreap Deployment on the PVC |
| `kv-events-policy/` | optional kustomize Component, not for OpenDataHub/RHOAI: a NetworkPolicy limiting 5557/5558 to kvreap and the EPP |
| `kvreap-monitoring/` | optional kustomize Component: a PodMonitor for kvreap's metrics and a NetworkPolicy for its port |
| `kvreap-metrics-rbac/` | optional kustomize Component, instead of `kvreap-monitoring`: the same over TLS with token auth through kube-rbac-proxy |
| `example/` | an overlay that includes the preset and the kvreap Component for a service named `qwen3-32b` |

```bash
kubectl kustomize deploy/kserve/example
```

## What the service needs

- `spec.kvCacheOffloading.secondary[0].fileSystem.pvc.ref` naming an RWX PVC.
  kvreap mounts the same PVC; set `data.pvc` in your `kvreap-target` ConfigMap
  to that name. If the ref sets a `path`, set kvreap's `CACHE_DIRECTORY` to it.
- kvreap must run as the uid vLLM writes the cache with, or it cannot unlink
  in vLLM's directories. On OpenShift (OpenDataHub/RHOAI) leave the uid unset:
  the restricted-v2 SCC gives both pods the namespace's uid. Elsewhere patch
  the Deployment's `runAsUser` (or a shared `fsGroup`) to vLLM's.
- `spec.baseRefs: [{name: kv-offloading-kvreap}]`. The preset makes vLLM bind
  its KV events PUB socket on 5557 (`tcp://*:5557`) and replay socket on 5558,
  send full block hashes
  (`VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES=0`), and turn on the fs tier's own
  `STORAGE` events. It replaces the `--kv-transfer-config` KServe renders from
  `kvCacheOffloading`, because the rendered one has no `enable_kv_events`;
  keep `cpu_bytes_to_use` in line with `kvCacheOffloading.cpu`. With a KServe
  that has `secondary[].fileSystem.kvEvents` (wseaton/kserve branch
  `llmisvc-fs-tier-kv-events`), set `kvEvents: true` on the tier and drop the
  `--kv-transfer-config` from the preset. RHOAI's KServe module ships the CRDs
  vendored from a release ref, so the field reaches RHOAI only after that
  copy is refreshed, not when the code merges.
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

## Metrics

kvreap serves Prometheus metrics on port 9090 (`METRICS_ADDR`, plain HTTP,
`GET /metrics`): deletions, metadata-op latency, prune duration, usage, the
delete grant and op-rate limit, chain deletions by position, and KV event
batches, gaps, replays and losses. Include `kvreap-monitoring` as a second
component for a PodMonitor and a NetworkPolicy that admits the same
monitoring namespaces OpenDataHub admits for vLLM (`openshift-monitoring`,
`openshift-user-workload-monitoring`, `redhat-ods-monitoring`); it needs the
Prometheus Operator CRDs.
For a TLS scrape with token auth, include `kvreap-metrics-rbac` instead. It
binds kvreap to loopback and adds the kube-rbac-proxy sidecar OpenDataHub
uses for InferenceService pods, serving HTTPS on 8443 and admitting only
tokens allowed to get the `/metrics` non-resource URL. It brings:

- a ServiceAccount for kvreap bound to `system:auth-delegator`, so the proxy
  can run TokenReviews and SubjectAccessReviews;
- a scraper ServiceAccount with a token Secret, bound to the
  `kvreap-metrics-reader` ClusterRole, which the PodMonitor presents;
- a Service whose `serving-cert-secret-name` annotation has the OpenShift
  service CA issue `<llmisvc>-kvreap-metrics-tls`. Elsewhere create that TLS
  Secret yourself (cert-manager, or `kubectl create secret tls`).

The two ClusterRoleBindings are cluster-scoped, so installing the component
needs cluster-admin; their names carry the LLMInferenceService name, so two
services of the same name in different namespaces collide. On RHOAI, swap the
proxy image for `registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9`.

## Security

BlockStored events carry prompt token ids. kvreap implements no ZMQ
authentication (NULL mechanism, no crypto in the binary), the same as the EPP's
own KV event subscription, so the events are plaintext on the pod network.

- **OpenDataHub/RHOAI.** The KServe controller gives each LLMInferenceService a
  NetworkPolicy that admits the gateway, the monitoring namespaces, and every
  pod in the service's namespace on any port. That covers kvreap and the EPP,
  and it also means any pod in the namespace can read 5557 and 5558: the
  namespace is the boundary. Workload pods are not in a service mesh by
  default, so there is no sidecar mTLS. Run the service in a namespace that
  holds only trusted workloads, and do not add `kv-events-policy`, which
  keeps 8000 open to all and would widen the controller's policy.
- **Elsewhere.** Include `kv-events-policy` to allow 5557 and 5558 only from
  kvreap and the EPP. It selects the workload pods for Ingress, so it lists
  8000 as open to all; add any other port your workload serves. In a mesh,
  keep the ports named `tcp-*` so the sidecars carry them as opaque TCP with
  mTLS.

## Optional: faster fs tier reads

vLLM 0.31's fs tier reads each promotion on one thread. Until vllm#58404 lands,
`bench/vllm_plugins/striped_fs.py` is an out-of-tree tier that splits loads
across the I/O pool (returning-turn TTFT p50 5.7 s to 0.86 s on Qwen3-32B over
VAST); load it with `"type": "StripedFileSystemTierManager",
"module_path": "striped_fs"` in the preset's tier and the file on vLLM's
`PYTHONPATH`.

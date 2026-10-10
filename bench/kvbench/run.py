"""Run one evictor variant end to end on a Kubernetes cluster.

```text
 kv PVC (VAST, nosharecache) ──────────────┬──────────────────────────┐
   │                                       │                          │
 vLLM pod (1 GPU)                        evictor pod (Helm chart)     │
   vllm serve + TieringOffloadingSpec      pvc-evictor / kvreap       │
   sampler sidecar: mountstats,            sampler sidecar:           │
     statvfs, /metrics                       mountstats               │
   ▲                                                                  │
 loadgen pod: churn (unique prompts) + hot (repeated prefixes)        │
                                                                      │
 nosharecache gives each pod its own NFS superblock, so each sampler ─┘
 sees only its own pod's NFS operations.
```
"""

from __future__ import annotations

import json
import random
import subprocess
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

PART_OF = "kvreap-bench"
NO_ISTIO = {"sidecar.istio.io/inject": "false"}
SAMPLER_IMAGE = "docker.io/library/python:3.12-slim"
KV_MOUNT = "/kv-cache"
CACHE_DIRECTORY = "kv/model-cache/models"
KV_EVENTS_PORT = 5557

Manifest = dict[str, Any]
Placement = Literal["any", "colocated", "separate"]
Workload = Literal["churn-hot", "agent"]
NYANN_IMAGE = "quay.io/wseaton/nyann-bench:kvbench-edd5f54"


@dataclass(frozen=True)
class Cluster:
    context: str
    namespace: str

    def kubectl(self, *args: str, stdin: str | None = None, check: bool = True) -> str:
        cmd = ["kubectl", "--context", self.context, "-n", self.namespace, *args]
        res = subprocess.run(cmd, input=stdin, capture_output=True, text=True, check=False)
        if check and res.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)} failed:\n{res.stderr}")
        return res.stdout

    def apply(self, objs: list[Manifest]) -> None:
        self.kubectl("apply", "-f", "-", stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": objs}))

    def delete(self, objs: list[Manifest]) -> None:
        self.kubectl(
            "delete",
            "--ignore-not-found",
            "--wait=true",
            "-f",
            "-",
            stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": objs}),
            check=False,
        )

    def exists(self, kind: str, name: str) -> bool:
        return bool(self.kubectl("get", kind, name, "--ignore-not-found", "-o", "name").strip())


@dataclass(frozen=True)
class RunConfig:
    variant: str
    out: Path
    evictor_image: str | None
    chart: Path | None
    cluster: Cluster
    model: str = "Qwen/Qwen3-0.6B"
    vllm_image: str = "docker.io/vllm/vllm-openai:v0.31.0"
    storage_class: str = "kvreap-bench-vast"
    pvc_size: str = "100Gi"
    hf_pvc: str = "kvreap-bench-hf"
    hf_pvc_size: str = "50Gi"
    tensor_parallel_size: int = 1
    gpu_memory_utilization: float = 0.3
    pull_secret: str = "quay-wseaton-pull"
    duration_s: int = 1200
    settle_s: int = 60
    offload_block_tokens: int = 16
    cpu_tier_bytes: int = 2 * 1024**3
    gpu_blocks: int = 4096
    max_model_len: int = 8192
    churn_input_len: int = 4096
    churn_concurrency: int = 8
    hot_prefixes: int = 64
    hot_prefix_len: int = 2048
    hot_request_rate: float = 2.0
    evictor_values: dict[str, Any] = field(default_factory=dict)
    placement: Placement = "any"
    share_nfs_client: bool = False
    kv_events: bool = False
    workload: Workload = "churn-hot"
    agent_concurrency: int = 8
    agent_pool: int = 64
    agent_first_isl: int = 16000
    agent_turn_isl: int = 500
    agent_osl: int = 200
    agent_turns: int = 10
    agent_system_prompt_tokens: int = 2048
    kv_cache_dtype: str | None = None
    fs_read_threads: int = 16
    vllm_memory_gib: int = 96
    evictor_env: dict[str, str] = field(default_factory=dict)

    @property
    def run_id(self) -> str:
        return f"{PART_OF}-{self.variant}"

    @property
    def kv_pvc(self) -> str:
        return f"{self.run_id}-kv"

    @property
    def kv_events_endpoint(self) -> str:
        return f"tcp://{self.run_id}-vllm:{KV_EVENTS_PORT}"


def labels(cfg: RunConfig, role: str) -> dict[str, str]:
    return {"app.kubernetes.io/part-of": PART_OF, "kvreap-bench/run": cfg.variant, "kvreap-bench/role": role}


def ensure_storage_class(cluster: Cluster, name: str, nosharecache: bool, source: str = "shared-vast") -> None:
    """Clone `source` with reclaimPolicy Delete, optionally adding nosharecache."""
    if cluster.exists("storageclass", name):
        return
    sc = json.loads(cluster.kubectl("get", "storageclass", source, "-o", "json"))
    options = [o for o in sc.get("mountOptions", []) if o != "nosharecache"]
    if nosharecache:
        options.append("nosharecache")
    cluster.apply(
        [
            {
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {"name": name, "labels": {"app.kubernetes.io/part-of": PART_OF}},
                "provisioner": sc["provisioner"],
                "parameters": sc.get("parameters", {}),
                "mountOptions": options,
                "reclaimPolicy": "Delete",
                "volumeBindingMode": "Immediate",
                "allowVolumeExpansion": True,
            }
        ]
    )


def pvc(name: str, storage_class: str, size: str, lbl: dict[str, str]) -> Manifest:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "labels": lbl},
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "storageClassName": storage_class,
            "resources": {"requests": {"storage": size}},
        },
    }


def agent_preamble(tokens: int, seed: int = 42) -> str:
    """Fixed agent system prompt of roughly `tokens` tokens (about 0.75 words per token)."""
    words = [
        "tool",
        "call",
        "result",
        "file",
        "path",
        "search",
        "query",
        "function",
        "argument",
        "return",
        "value",
        "error",
        "retry",
        "plan",
        "step",
        "context",
        "repository",
        "branch",
        "commit",
        "diff",
        "test",
        "build",
        "deploy",
        "config",
        "parameter",
        "schema",
        "request",
        "response",
    ]
    rng = random.Random(seed)
    return " ".join(rng.choice(words) for _ in range(tokens * 3 // 4))


def nyann_config(cfg: RunConfig) -> dict[str, Any]:
    """nyann-bench conversation-pool scenario: agent sessions that share a preamble and come back
    only after the rest of the pool has had a turn."""
    return {
        "load": {
            "mode": "conversation_pool",
            "concurrency": cfg.agent_concurrency,
            "conversation_pool_size": cfg.agent_pool,
            "rampup": "60s",
            "duration": f"{cfg.duration_s}s",
        },
        "workload": {
            "type": "synthetic",
            "isl": cfg.agent_first_isl,
            "subsequent_isl": cfg.agent_turn_isl,
            "osl": cfg.agent_osl,
            "turns": cfg.agent_turns,
            "system_prompt_file": "agent.txt",
        },
    }


def sampler_configmap(cfg: RunConfig) -> Manifest:
    data = {"sampler.py": (Path(__file__).parent / "sampler.py").read_text()}
    if cfg.workload == "agent":
        data["nyann.json"] = json.dumps(nyann_config(cfg))
        data["agent.txt"] = agent_preamble(cfg.agent_system_prompt_tokens)
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": f"{cfg.run_id}-sampler", "labels": labels(cfg, "sampler")},
        "data": data,
    }


def sampler_container(kv_volume: str, scrape_url: str | None = None) -> Manifest:
    cmd = ["python3", "/bench/sampler.py", "--mount", KV_MOUNT, "--interval", "1", "--output", SAMPLES_FILE]
    if scrape_url:
        cmd += ["--scrape-url", scrape_url]
    return {
        "name": "sampler",
        "image": SAMPLER_IMAGE,
        "command": cmd,
        "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}, "limits": {"memory": "256Mi"}},
        "volumeMounts": [
            {"name": kv_volume, "mountPath": KV_MOUNT, "readOnly": True},
            {"name": "bench", "mountPath": "/bench"},
            {"name": "samples", "mountPath": "/out"},
        ],
    }


SAMPLES_FILE = "/out/samples.jsonl"
SAMPLES_VOLUME: Manifest = {"name": "samples", "emptyDir": {}}


def bench_volume(cfg: RunConfig) -> Manifest:
    return {"name": "bench", "configMap": {"name": f"{cfg.run_id}-sampler"}}


def vllm_manifests(cfg: RunConfig) -> list[Manifest]:
    kv_config = {
        "kv_connector": "OffloadingConnector",
        "kv_role": "kv_both",
        "kv_connector_extra_config": {
            "spec_name": "TieringOffloadingSpec",
            "cpu_bytes_to_use": cfg.cpu_tier_bytes,
            "block_size": cfg.offload_block_tokens,
            "secondary_tiers": [
                {
                    "type": "fs",
                    "root_dir": f"{KV_MOUNT}/{CACHE_DIRECTORY}",
                    "n_read_threads": cfg.fs_read_threads,
                    "n_write_threads": 16,
                    **({"enable_kv_events": True} if cfg.kv_events else {}),
                }
            ],
        },
    }
    lbl = labels(cfg, "vllm")
    name = f"{cfg.run_id}-vllm"
    args = [
        "--port=8000",
        f"--max-model-len={cfg.max_model_len}",
        f"--gpu-memory-utilization={cfg.gpu_memory_utilization}",
        f"--num-gpu-blocks-override={cfg.gpu_blocks}",
        f"--kv-transfer-config={json.dumps(kv_config)}",
    ]
    if cfg.kv_cache_dtype:
        args.append(f"--kv-cache-dtype={cfg.kv_cache_dtype}")
    if cfg.tensor_parallel_size > 1:
        args.append(f"--tensor-parallel-size={cfg.tensor_parallel_size}")
    gpus = str(cfg.tensor_parallel_size)
    env = [
        {"name": "HF_HOME", "value": "/models/hf"},
        {"name": "HOME", "value": "/tmp"},
        {"name": "VLLM_LOGGING_LEVEL", "value": "INFO"},
    ]
    ports = [{"containerPort": 8000}]
    svc_ports = [{"name": "http", "port": 8000, "targetPort": 8000}]
    if cfg.kv_events:
        events = {"enable_kv_cache_events": True, "publisher": "zmq", "endpoint": f"tcp://*:{KV_EVENTS_PORT}"}
        args.append(f"--kv-events-config={json.dumps(events)}")
        env.append({"name": "VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES", "value": "0"})
        ports.append({"containerPort": KV_EVENTS_PORT})
        svc_ports.append({"name": "kv-events", "port": KV_EVENTS_PORT, "targetPort": KV_EVENTS_PORT})
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": lbl, "annotations": NO_ISTIO},
        "spec": {
            "restartPolicy": "Never",
            "securityContext": {
                "runAsUser": 1000,
                "runAsGroup": 1000,
                "fsGroup": 1000,
                "fsGroupChangePolicy": "OnRootMismatch",
            },
            "containers": [
                {
                    "name": "vllm",
                    "image": cfg.vllm_image,
                    "command": ["vllm", "serve", cfg.model],
                    "args": args,
                    "env": env,
                    "ports": ports,
                    "readinessProbe": {"httpGet": {"path": "/health", "port": 8000}, "periodSeconds": 5},
                    "resources": {
                        "requests": {
                            "nvidia.com/gpu": gpus,
                            "cpu": "16",
                            "memory": f"{min(64, cfg.vllm_memory_gib)}Gi",
                        },
                        "limits": {"nvidia.com/gpu": gpus, "memory": f"{cfg.vllm_memory_gib}Gi"},
                    },
                    "volumeMounts": [
                        {"name": "kv", "mountPath": KV_MOUNT},
                        {"name": "hf", "mountPath": "/models"},
                        {"name": "shm", "mountPath": "/dev/shm"},
                    ],
                },
                sampler_container("kv", scrape_url="http://127.0.0.1:8000/metrics"),
            ],
            "volumes": [
                {"name": "kv", "persistentVolumeClaim": {"claimName": cfg.kv_pvc}},
                {"name": "hf", "persistentVolumeClaim": {"claimName": cfg.hf_pvc}},
                {"name": "shm", "emptyDir": {"medium": "Memory", "sizeLimit": "16Gi"}},
                bench_volume(cfg),
                SAMPLES_VOLUME,
            ],
        },
    }
    svc = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "labels": lbl},
        "spec": {"selector": lbl, "ports": svc_ports},
    }
    return [pod, svc]


LOADGEN_SCRIPT = r"""
set -u
end=$((SECONDS + DURATION))
mkdir -p /results/churn /results/hot
common="--backend vllm --base-url $BASE_URL --model $MODEL --save-result --save-detailed --ignore-eos"
(
  i=0
  while [ $SECONDS -lt $end ]; do
    vllm bench serve $common --dataset-name random \
      --random-input-len $CHURN_INPUT_LEN --random-output-len 8 --num-prompts 64 \
      --max-concurrency $CHURN_CONCURRENCY --seed $((1000 + i)) \
      --result-dir /results/churn --result-filename churn-$i.json > /results/churn/log-$i.txt 2>&1 \
      || echo "churn iteration $i failed" >&2
    i=$((i + 1))
  done
) &
(
  i=0
  while [ $SECONDS -lt $end ]; do
    vllm bench serve $common --dataset-name prefix_repetition \
      --prefix-repetition-prefix-len $HOT_PREFIX_LEN --prefix-repetition-suffix-len 32 \
      --prefix-repetition-num-prefixes $HOT_PREFIXES --prefix-repetition-output-len 8 \
      --num-prompts $HOT_PREFIXES --request-rate $HOT_RATE --seed 7 \
      --result-dir /results/hot --result-filename hot-$i.json > /results/hot/log-$i.txt 2>&1 \
      || echo "hot iteration $i failed" >&2
    i=$((i + 1))
  done
) &
wait
touch /results/DONE
sleep infinity
"""


AGENT_SCRIPT = r"""
set -u
mkdir -p /results/agent
nyann-bench generate --target "$BASE_URL/v1" --model "$MODEL" --config /bench/nyann.json \
  --output-dir /results/agent > /results/agent/log.txt 2>&1 || echo "nyann-bench failed" >&2
touch /results/DONE
sleep infinity
"""


def agent_loadgen_manifest(cfg: RunConfig) -> Manifest:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": f"{cfg.run_id}-loadgen", "labels": labels(cfg, "loadgen"), "annotations": NO_ISTIO},
        "spec": {
            "restartPolicy": "Never",
            "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000},
            "imagePullSecrets": [{"name": cfg.pull_secret}],
            "containers": [
                {
                    "name": "loadgen",
                    "image": NYANN_IMAGE,
                    "command": ["sh", "-c", AGENT_SCRIPT],
                    "env": [
                        {"name": "BASE_URL", "value": f"http://{cfg.run_id}-vllm:8000"},
                        {"name": "MODEL", "value": cfg.model},
                    ],
                    "resources": {"requests": {"cpu": "4", "memory": "4Gi"}},
                    "volumeMounts": [
                        {"name": "bench", "mountPath": "/bench"},
                        {"name": "results", "mountPath": "/results"},
                    ],
                }
            ],
            "volumes": [bench_volume(cfg), {"name": "results", "emptyDir": {}}],
        },
    }


def loadgen_manifest(cfg: RunConfig) -> Manifest:
    if cfg.workload == "agent":
        return agent_loadgen_manifest(cfg)
    env = {
        "DURATION": str(cfg.duration_s),
        "BASE_URL": f"http://{cfg.run_id}-vllm:8000",
        "MODEL": cfg.model,
        "CHURN_INPUT_LEN": str(cfg.churn_input_len),
        "CHURN_CONCURRENCY": str(cfg.churn_concurrency),
        "HOT_PREFIXES": str(cfg.hot_prefixes),
        "HOT_PREFIX_LEN": str(cfg.hot_prefix_len),
        "HOT_RATE": str(cfg.hot_request_rate),
        "HF_HOME": "/models/hf",
        "HF_HUB_OFFLINE": "1",
        "HOME": "/tmp",
    }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": f"{cfg.run_id}-loadgen", "labels": labels(cfg, "loadgen"), "annotations": NO_ISTIO},
        "spec": {
            "restartPolicy": "Never",
            "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000},
            "containers": [
                {
                    "name": "loadgen",
                    "image": cfg.vllm_image,
                    "command": ["bash", "-c", LOADGEN_SCRIPT],
                    "env": [{"name": k, "value": v} for k, v in env.items()],
                    "resources": {"requests": {"cpu": "8", "memory": "16Gi"}},
                    "volumeMounts": [
                        {"name": "hf", "mountPath": "/models", "readOnly": True},
                        {"name": "results", "mountPath": "/results"},
                    ],
                }
            ],
            "volumes": [
                {"name": "hf", "persistentVolumeClaim": {"claimName": cfg.hf_pvc}},
                {"name": "results", "emptyDir": {}},
            ],
        },
    }


def split_image(ref: str) -> tuple[str, str]:
    repo, _, tag = ref.rpartition(":")
    if not repo or "/" in tag:
        raise ValueError(f"image must be repo:tag, got {ref}")
    return repo, tag


def deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in extra.items():
        out[k] = deep_merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def evictor_manifests(cfg: RunConfig) -> list[Manifest]:
    if cfg.evictor_image is None or cfg.chart is None:
        return []
    repo, tag = split_image(cfg.evictor_image)
    values = deep_merge(
        {
            "image": {"repository": repo, "tag": tag, "pullPolicy": "IfNotPresent"},
            "pvc": {"name": cfg.kv_pvc, "mountPath": KV_MOUNT, "readOnly": False},
            "securityContext": {
                "pod": {"fsGroup": 1000, "seLinuxOptions": {"level": "s0"}},
                "container": {"runAsUser": 1000},
            },
            "config": {
                "cacheDirectory": CACHE_DIRECTORY,
                "fileAccessTimeThresholdMinutes": 2,
                "logFilePath": "",
            },
            "labels": labels(cfg, "evictor"),
        },
        cfg.evictor_values,
    )
    rendered = subprocess.run(
        ["helm", "template", f"{cfg.run_id}-evictor", str(cfg.chart), "-f", "-"],
        input=yaml.safe_dump(values),
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    objs: list[Manifest] = [d for d in yaml.safe_load_all(rendered) if d]
    for obj in objs:
        obj.setdefault("metadata", {}).pop("namespace", None)
        if obj["kind"] != "Deployment":
            continue
        tmpl = obj["spec"]["template"]
        tmpl.setdefault("metadata", {}).setdefault("annotations", {}).update(NO_ISTIO)
        spec = tmpl["spec"]
        spec["imagePullSecrets"] = [{"name": cfg.pull_secret}]
        spec["volumes"] += [bench_volume(cfg), SAMPLES_VOLUME]
        env = dict(cfg.evictor_env)
        if cfg.kv_events:
            env.setdefault("KV_EVENTS_ENDPOINTS", cfg.kv_events_endpoint)
            env.setdefault("KV_EVENTS_DISK_MEDIUM", "STORAGE")
        for c in spec["containers"]:
            if c["name"] == "evictor":
                c.setdefault("env", []).extend({"name": k, "value": v} for k, v in env.items())
        spec["containers"].append(sampler_container("kv-cache-storage"))
        if cfg.placement != "any":
            kind = "podAffinity" if cfg.placement == "colocated" else "podAntiAffinity"
            spec["affinity"] = {
                kind: {
                    "requiredDuringSchedulingIgnoredDuringExecution": [
                        {"labelSelector": {"matchLabels": labels(cfg, "vllm")}, "topologyKey": "kubernetes.io/hostname"}
                    ]
                }
            }
    return objs


def wait_pod_ready(cluster: Cluster, name: str, timeout_s: int) -> None:
    cluster.kubectl("wait", f"pod/{name}", "--for=condition=Ready", f"--timeout={timeout_s}s")


def wait_loadgen_done(cfg: RunConfig, timeout_s: int) -> None:
    name = f"{cfg.run_id}-loadgen"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        res = subprocess.run(
            [
                "kubectl",
                "--context",
                cfg.cluster.context,
                "-n",
                cfg.cluster.namespace,
                "exec",
                name,
                "--",
                "test",
                "-f",
                "/results/DONE",
            ],
            capture_output=True,
            check=False,
        )
        if res.returncode == 0:
            return
        time.sleep(30)
    raise TimeoutError(f"loadgen did not finish within {timeout_s}s")


def collect(cfg: RunConfig, evictor_pod: str | None, events: dict[str, float]) -> None:
    c = cfg.cluster
    out = cfg.out
    out.mkdir(parents=True, exist_ok=True)
    vllm = f"{cfg.run_id}-vllm"
    c.kubectl("cp", f"{vllm}:{SAMPLES_FILE}", str(out / "vllm-sampler.jsonl"), "-c", "sampler")
    (out / "vllm.log").write_text(c.kubectl("logs", vllm, "-c", "vllm", "--timestamps"))
    if evictor_pod:
        c.kubectl("cp", f"{evictor_pod}:{SAMPLES_FILE}", str(out / "evictor-sampler.jsonl"), "-c", "sampler")
        (out / "evictor.log").write_text(c.kubectl("logs", evictor_pod, "-c", "evictor", "--timestamps"))
    c.kubectl("cp", f"{cfg.run_id}-loadgen:/results", str(out / "loadgen"), "-c", "loadgen")
    meta = {k: (str(v) if isinstance(v, (Path, Cluster)) else v) for k, v in asdict(cfg).items()}
    meta["events"] = events
    meta["nodes"] = {
        "vllm": c.kubectl("get", "pod", vllm, "-o", "jsonpath={.spec.nodeName}").strip(),
        "evictor": c.kubectl("get", "pod", evictor_pod, "-o", "jsonpath={.spec.nodeName}").strip()
        if evictor_pod
        else None,
    }
    (out / "meta.json").write_text(json.dumps(meta, indent=2, default=str))


def teardown(cfg: RunConfig, evictor: list[Manifest]) -> None:
    c = cfg.cluster
    c.delete([loadgen_manifest(cfg)])
    c.delete(evictor)
    c.delete(vllm_manifests(cfg))
    c.delete([sampler_configmap(cfg), pvc(cfg.kv_pvc, cfg.storage_class, cfg.pvc_size, {})])


def run(cfg: RunConfig, keep: bool = False) -> None:
    c = cfg.cluster
    ensure_storage_class(c, cfg.storage_class, nosharecache=not cfg.share_nfs_client)
    if not c.exists("pvc", cfg.hf_pvc):
        c.apply([pvc(cfg.hf_pvc, "shared-vast", cfg.hf_pvc_size, {"app.kubernetes.io/part-of": PART_OF})])

    evictor = evictor_manifests(cfg)
    print(f"[{cfg.variant}] cleaning up any previous run", flush=True)
    teardown(cfg, evictor)

    events: dict[str, float] = {}
    c.apply([pvc(cfg.kv_pvc, cfg.storage_class, cfg.pvc_size, labels(cfg, "kv")), sampler_configmap(cfg)])
    c.apply(vllm_manifests(cfg))
    print(f"[{cfg.variant}] waiting for vLLM", flush=True)
    wait_pod_ready(c, f"{cfg.run_id}-vllm", timeout_s=1800)
    events["vllm_ready"] = time.time()

    evictor_pod = None
    try:
        if evictor:
            c.apply(evictor)
            c.kubectl("rollout", "status", f"deployment/{cfg.run_id}-evictor-pvc-evictor", "--timeout=300s")
            evictor_pod = c.kubectl(
                "get",
                "pods",
                "-l",
                f"kvreap-bench/run={cfg.variant},kvreap-bench/role=evictor",
                "-o",
                "jsonpath={.items[0].metadata.name}",
            ).strip()
        events["evictor_ready"] = time.time()

        c.apply([loadgen_manifest(cfg)])
        wait_pod_ready(c, f"{cfg.run_id}-loadgen", timeout_s=600)
        events["load_start"] = time.time()
        print(f"[{cfg.variant}] load running for {cfg.duration_s}s", flush=True)
        wait_loadgen_done(cfg, timeout_s=cfg.duration_s + 900)
        events["load_end"] = time.time()
        time.sleep(cfg.settle_s)
        events["collect"] = time.time()
        collect(cfg, evictor_pod, events)
        print(f"[{cfg.variant}] results in {cfg.out}", flush=True)
    finally:
        if not keep:
            teardown(cfg, evictor)

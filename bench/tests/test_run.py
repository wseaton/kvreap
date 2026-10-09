import json
import shutil
from pathlib import Path

import pytest

from kvbench.run import KV_EVENTS_PORT, Cluster, RunConfig, evictor_manifests, vllm_manifests

CHART = Path.home() / "git/llm-d-kv-cache-evictor/kv_connectors/pvc_evictor/helm"


def cfg(**kw) -> RunConfig:
    return RunConfig(
        variant="d",
        out=Path("/tmp/out"),
        evictor_image="quay.io/x/kvreap:t",
        chart=CHART,
        cluster=Cluster("ctx", "ns"),
        **kw,
    )


def vllm_parts(c: RunConfig) -> tuple[dict, dict]:
    pod, svc = vllm_manifests(c)
    return pod["spec"]["containers"][0], svc


def test_vllm_without_kv_events_is_unchanged() -> None:
    container, svc = vllm_parts(cfg())
    assert not any(a.startswith("--kv-events-config") for a in container["args"])
    assert not any(e["name"] == "VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES" for e in container["env"])
    assert container["ports"] == [{"containerPort": 8000}]
    assert [p["port"] for p in svc["spec"]["ports"]] == [8000]


def test_vllm_with_kv_events_publishes_on_the_service() -> None:
    container, svc = vllm_parts(cfg(kv_events=True))
    [arg] = [a for a in container["args"] if a.startswith("--kv-events-config=")]
    assert json.loads(arg.split("=", 1)[1]) == {
        "enable_kv_cache_events": True,
        "publisher": "zmq",
        "endpoint": f"tcp://*:{KV_EVENTS_PORT}",
    }
    assert {"containerPort": KV_EVENTS_PORT} in container["ports"]
    assert {"name": "VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES", "value": "0"} in container["env"]
    assert {"name": "kv-events", "port": KV_EVENTS_PORT, "targetPort": KV_EVENTS_PORT} in svc["spec"]["ports"]
    assert cfg().kv_events_endpoint == f"tcp://kvreap-bench-d-vllm:{KV_EVENTS_PORT}"
    [kv] = [a for a in container["args"] if a.startswith("--kv-transfer-config=")]
    [tier] = json.loads(kv.split("=", 1)[1])["kv_connector_extra_config"]["secondary_tiers"]
    assert tier["enable_kv_events"] is True
    plain, _ = vllm_parts(cfg())
    [kv] = [a for a in plain["args"] if a.startswith("--kv-transfer-config=")]
    assert "enable_kv_events" not in kv


def evictor_env(c: RunConfig) -> dict[str, str]:
    [dep] = [o for o in evictor_manifests(c) if o["kind"] == "Deployment"]
    [ev] = [x for x in dep["spec"]["template"]["spec"]["containers"] if x["name"] == "evictor"]
    return {e["name"]: e.get("value") for e in ev.get("env", [])}


needs_chart = pytest.mark.skipif(
    shutil.which("helm") is None or not CHART.is_dir(), reason="needs helm and the pvc-evictor chart"
)


@needs_chart
def test_evictor_gets_kv_events_endpoint_and_extra_env() -> None:
    env = evictor_env(cfg(kv_events=True, evictor_env={"CHAIN_EVICTION": "observe"}))
    assert env["KV_EVENTS_ENDPOINTS"] == "tcp://kvreap-bench-d-vllm:5557"
    assert env["CHAIN_EVICTION"] == "observe"
    assert env["KV_EVENTS_DISK_MEDIUM"] == "STORAGE"


@needs_chart
def test_explicit_endpoint_wins_and_default_has_none() -> None:
    env = evictor_env(cfg(kv_events=True, evictor_env={"KV_EVENTS_ENDPOINTS": "tcp://elsewhere:1"}))
    assert env["KV_EVENTS_ENDPOINTS"] == "tcp://elsewhere:1"
    assert "KV_EVENTS_ENDPOINTS" not in evictor_env(cfg())


def test_tensor_parallel_requests_gpus_and_passes_the_flag() -> None:
    container, _ = vllm_parts(cfg(tensor_parallel_size=4))
    assert "--tensor-parallel-size=4" in container["args"]
    assert "--gpu-memory-utilization=0.3" in container["args"]
    tuned, _ = vllm_parts(cfg(gpu_memory_utilization=0.6))
    assert "--gpu-memory-utilization=0.6" in tuned["args"]
    assert container["resources"]["requests"]["nvidia.com/gpu"] == "4"
    assert container["resources"]["limits"]["nvidia.com/gpu"] == "4"
    single, _ = vllm_parts(cfg())
    assert not any(a.startswith("--tensor-parallel-size") for a in single["args"])
    assert single["resources"]["limits"]["nvidia.com/gpu"] == "1"


def test_hf_pvc_name_reaches_vllm_and_loadgen() -> None:
    from kvbench.run import loadgen_manifest

    c = cfg(hf_pvc="kvreap-bench-hf-large")
    pod, _ = vllm_manifests(c)
    claims = {v["persistentVolumeClaim"]["claimName"] for v in pod["spec"]["volumes"] if "persistentVolumeClaim" in v}
    assert "kvreap-bench-hf-large" in claims
    lg = loadgen_manifest(c)
    assert lg["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] == "kvreap-bench-hf-large"


def test_shared_prefix_settings_reach_the_loadgen() -> None:
    from kvbench.run import loadgen_manifest

    lg = loadgen_manifest(cfg(churn_shared_prefix_len=1024, churn_shared_prefixes=4))
    env = {e["name"]: e["value"] for e in lg["spec"]["containers"][0]["env"]}
    assert env["SHARED_PREFIX_LEN"] == "1024"
    assert env["SHARED_PREFIXES"] == "4"
    script = lg["spec"]["containers"][0]["command"][-1]
    assert "--dataset-name custom" in script
    assert "--skip-chat-template" in script
    plain = {e["name"]: e["value"] for e in loadgen_manifest(cfg())["spec"]["containers"][0]["env"]}
    assert plain["SHARED_PREFIX_LEN"] == "0"

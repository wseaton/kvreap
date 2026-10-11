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
    env = evictor_env(cfg(kv_events=True, evictor_env={"DELETE_FANOUT": "4"}))
    assert env["KV_EVENTS_ENDPOINTS"] == "tcp://kvreap-bench-d-vllm:5557"
    assert env["DELETE_FANOUT"] == "4"
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


def test_prompt_lengths_and_context_reach_vllm_and_loadgen() -> None:
    from kvbench.run import loadgen_manifest

    c = cfg(churn_input_len=16384, hot_prefix_len=8192, max_model_len=20480, gpu_blocks=12288)
    container, _ = vllm_parts(c)
    assert "--max-model-len=20480" in container["args"]
    assert "--num-gpu-blocks-override=12288" in container["args"]
    env = {e["name"]: e["value"] for e in loadgen_manifest(c)["spec"]["containers"][0]["env"]}
    assert env["CHURN_INPUT_LEN"] == "16384"
    assert env["HOT_PREFIX_LEN"] == "8192"
    default, _ = vllm_parts(cfg())
    assert "--max-model-len=8192" in default["args"]


def test_agent_workload_runs_nyann_conversation_pool() -> None:
    from kvbench.run import NYANN_IMAGE, loadgen_manifest, nyann_config, sampler_configmap

    c = cfg(workload="agent", agent_pool=96, agent_first_isl=12000, duration_s=600)
    conf = nyann_config(c)
    assert conf["load"]["mode"] == "conversation_pool"
    assert conf["load"]["conversation_pool_size"] == 96
    assert conf["load"]["duration"] == "600s"
    assert conf["workload"]["isl"] == 12000
    assert conf["workload"]["system_prompt_file"] == "agent.txt"
    data = sampler_configmap(c)["data"]
    assert json.loads(data["nyann.json"]) == conf
    assert 1000 < len(data["agent.txt"].split()) < 2000
    lg = loadgen_manifest(c)["spec"]["containers"][0]
    assert lg["image"] == NYANN_IMAGE
    assert "/bench/nyann.json" in lg["command"][-1]
    assert "touch /results/DONE" in lg["command"][-1]
    assert "--seed" not in lg["command"][-1], "nyann-bench edd5f54 has no --seed"
    assert "nyann.json" not in sampler_configmap(cfg())["data"]


def test_fp8_kv_and_read_threads_reach_vllm() -> None:
    container, _ = vllm_parts(cfg(kv_cache_dtype="fp8", fs_read_threads=64))
    assert "--kv-cache-dtype=fp8" in container["args"]
    [kv] = [a for a in container["args"] if a.startswith("--kv-transfer-config=")]
    assert json.loads(kv.split("=", 1)[1])["kv_connector_extra_config"]["secondary_tiers"][0]["n_read_threads"] == 64
    plain, _ = vllm_parts(cfg())
    assert not any(a.startswith("--kv-cache-dtype") for a in plain["args"])


def test_vllm_memory_limit_follows_the_flag() -> None:
    container, _ = vllm_parts(cfg(vllm_memory_gib=160))
    assert container["resources"]["limits"]["memory"] == "160Gi"
    assert vllm_parts(cfg())[0]["resources"]["limits"]["memory"] == "96Gi"


def test_striped_tier_ships_the_plugin_and_sizes_shm() -> None:
    from kvbench.run import sampler_configmap

    c = cfg(fs_stripe_blocks=4, cpu_tier_bytes=32 * 1024**3, vllm_memory_gib=160)
    container, _ = vllm_parts(c)
    [kv] = [a for a in container["args"] if a.startswith("--kv-transfer-config=")]
    [tier] = json.loads(kv.split("=", 1)[1])["kv_connector_extra_config"]["secondary_tiers"]
    assert tier["type"] == "StripedFileSystemTierManager"
    assert tier["module_path"] == "striped_fs"
    assert tier["stripe_blocks"] == 4
    assert {"name": "PYTHONPATH", "value": "/bench"} in container["env"]
    assert {"name": "bench", "mountPath": "/bench"} in container["volumeMounts"]
    assert "class StripedFileSystemTierManager" in sampler_configmap(c)["data"]["striped_fs.py"]
    pod, _ = vllm_manifests(c)
    [shm] = [v for v in pod["spec"]["volumes"] if v["name"] == "shm"]
    assert shm["emptyDir"]["sizeLimit"] == "40Gi"

    stock, _ = vllm_parts(cfg())
    [kv] = [a for a in stock["args"] if a.startswith("--kv-transfer-config=")]
    assert json.loads(kv.split("=", 1)[1])["kv_connector_extra_config"]["secondary_tiers"][0]["type"] == "fs"
    assert "striped_fs.py" not in sampler_configmap(cfg())["data"]
    [shm] = [v for v in vllm_manifests(cfg())[0]["spec"]["volumes"] if v["name"] == "shm"]
    assert shm["emptyDir"]["sizeLimit"] == "16Gi"


def test_host_path_cache_replaces_the_pvc_and_pins_the_node() -> None:
    c = cfg(kv_host_path="/mnt/local/kvreap-bench", node="gd91fda", pvc_size="160Gi")
    pod, _ = vllm_manifests(c)
    spec = pod["spec"]
    [kv] = [v for v in spec["volumes"] if v["name"] == "kv"]
    assert kv == {"name": "kv", "hostPath": {"path": "/mnt/local/kvreap-bench/d", "type": "DirectoryOrCreate"}}
    assert spec["nodeSelector"] == {"kubernetes.io/hostname": "gd91fda"}
    [init] = spec["initContainers"]
    assert init["securityContext"]["runAsUser"] == 0
    assert "chown 1000:1000" in init["command"][-1] and "-delete" in init["command"][-1]
    sampler = spec["containers"][1]["command"]
    assert sampler[sampler.index("--capacity-bytes") + 1] == str(160 * 1024**3)


def test_pvc_cache_has_no_init_container_or_node_pin() -> None:
    pod, _ = vllm_manifests(cfg())
    assert "initContainers" not in pod["spec"]
    assert "nodeSelector" not in pod["spec"]
    assert "--capacity-bytes" not in pod["spec"]["containers"][1]["command"]


def tiered() -> RunConfig:
    return cfg(kv_host_path="/mnt/local/kvreap-bench", pvc_size="80Gi", shared_tier_size="160Gi", kv_events=True)


def test_tiered_vllm_writes_the_host_path_then_the_shared_pvc() -> None:
    from kvbench.run import CACHE_DIRECTORY, KV_MOUNT, SHARED_KV_MOUNT

    c = tiered()
    assert c.tiered and c.kv_pvc_size == "160Gi"
    pod, _ = vllm_manifests(c)
    container = pod["spec"]["containers"][0]
    [kv] = [a for a in container["args"] if a.startswith("--kv-transfer-config=")]
    local, shared = json.loads(kv.split("=", 1)[1])["kv_connector_extra_config"]["secondary_tiers"]
    assert (local["root_dir"], local["locality"]) == (f"{KV_MOUNT}/{CACHE_DIRECTORY}", "LOCAL")
    assert (shared["root_dir"], shared["locality"]) == (f"{SHARED_KV_MOUNT}/{CACHE_DIRECTORY}", "REMOTE")
    assert local["enable_kv_events"] and shared["enable_kv_events"]
    assert {"name": "kv-shared", "mountPath": SHARED_KV_MOUNT} in container["volumeMounts"]
    volumes = {v["name"]: v for v in pod["spec"]["volumes"]}
    assert volumes["kv"]["hostPath"]["path"] == "/mnt/local/kvreap-bench/d"
    assert volumes["kv-shared"]["persistentVolumeClaim"]["claimName"] == c.kv_pvc

    single, _ = vllm_parts(cfg(kv_host_path="/mnt/local/kvreap-bench", kv_events=True))
    [kv] = [a for a in single["args"] if a.startswith("--kv-transfer-config=")]
    [tier] = json.loads(kv.split("=", 1)[1])["kv_connector_extra_config"]["secondary_tiers"]
    assert "locality" not in tier
    assert not any(m["name"] == "kv-shared" for m in single["volumeMounts"])
    assert cfg(kv_host_path="/mnt/local/kvreap-bench").kv_pvc_size is None
    assert cfg(pvc_size="100Gi").kv_pvc_size == "100Gi"


@needs_chart
def test_tiered_runs_one_evictor_per_tier() -> None:
    from kvbench.run import evictor_tiers

    c = tiered()
    deps = {o["metadata"]["name"]: o for o in evictor_manifests(c) if o["kind"] == "Deployment"}
    assert set(deps) == {t.deployment(c) for t in evictor_tiers(c)}
    by_role = {d["spec"]["template"]["metadata"]["labels"]["kvreap-bench/role"]: d for d in deps.values()}
    assert set(by_role) == {"evictor", "evictor-shared"}

    def env(role: str) -> dict[str, str]:
        [ev] = [x for x in by_role[role]["spec"]["template"]["spec"]["containers"] if x["name"] == "evictor"]
        return {e["name"]: e.get("value") for e in ev.get("env", [])}

    assert env("evictor")["KV_EVENTS_LOCALITY"] == "LOCAL"
    assert env("evictor")["CAPACITY_BYTES"] == str(80 * 1024**3)
    assert env("evictor-shared")["KV_EVENTS_LOCALITY"] == "REMOTE"
    assert "CAPACITY_BYTES" not in env("evictor-shared")
    for role in by_role:
        assert env(role)["KV_EVENTS_ENDPOINTS"] == c.kv_events_endpoint
    local = by_role["evictor"]["spec"]["template"]["spec"]
    shared = by_role["evictor-shared"]["spec"]["template"]["spec"]
    assert "podAffinity" in local["affinity"]
    assert "affinity" not in shared
    assert {"path": "/mnt/local/kvreap-bench/d", "type": "DirectoryOrCreate"} in [
        v.get("hostPath") for v in local["volumes"]
    ]
    assert c.kv_pvc in [v.get("persistentVolumeClaim", {}).get("claimName") for v in shared["volumes"]]
    assert "KV_EVENTS_LOCALITY" not in evictor_env(cfg(kv_events=True))


@needs_chart
def test_shared_tier_values_reach_only_the_shared_evictor() -> None:
    from dataclasses import replace

    c = replace(tiered(), shared_tier_values={"config": {"cleanupThreshold": 80, "targetThreshold": 65}})
    deps = [o for o in evictor_manifests(c) if o["kind"] == "Deployment"]

    def env(dep: dict) -> dict[str, str]:
        [ev] = [x for x in dep["spec"]["template"]["spec"]["containers"] if x["name"] == "evictor"]
        return {e["name"]: e.get("value") for e in ev.get("env", [])}

    by_role = {d["spec"]["template"]["metadata"]["labels"]["kvreap-bench/role"]: env(d) for d in deps}
    assert float(by_role["evictor-shared"]["CLEANUP_THRESHOLD"]) == 80
    assert float(by_role["evictor-shared"]["TARGET_THRESHOLD"]) == 65
    assert float(by_role["evictor"]["CLEANUP_THRESHOLD"]) == 85


@needs_chart
def test_host_path_evictor_runs_on_vllms_node_with_capacity_bytes() -> None:
    c = cfg(kv_host_path="/mnt/local/kvreap-bench", pvc_size="160Gi")
    [dep] = [o for o in evictor_manifests(c) if o["kind"] == "Deployment"]
    spec = dep["spec"]["template"]["spec"]
    assert not any(
        "persistentVolumeClaim" in v and v["persistentVolumeClaim"]["claimName"] == c.kv_pvc for v in spec["volumes"]
    )
    assert {"path": "/mnt/local/kvreap-bench/d", "type": "DirectoryOrCreate"} in [
        v.get("hostPath") for v in spec["volumes"]
    ]
    [term] = spec["affinity"]["podAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]
    assert term["labelSelector"]["matchLabels"]["kvreap-bench/role"] == "vllm"
    assert evictor_env(c)["CAPACITY_BYTES"] == str(160 * 1024**3)
    assert "CAPACITY_BYTES" not in evictor_env(cfg())

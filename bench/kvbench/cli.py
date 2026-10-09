import argparse
import json
import sys
from pathlib import Path

from kvbench import analyze, chart
from kvbench.run import KV_EVENTS_PORT, Cluster, RunConfig, run


def main() -> int:
    p = argparse.ArgumentParser(prog="kvbench", description="PVC evictor A/B/C benchmark against vLLM on Kubernetes")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="run one variant end to end")
    r.add_argument("--variant", required=True, help="short name, e.g. a, b, c or none")
    r.add_argument("--image", help="evictor image repo:tag (omit for --variant none)")
    r.add_argument("--chart", type=Path, help="path to the pvc-evictor Helm chart")
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--context", default="coreweave-waldorf")
    r.add_argument("--namespace", default="weaton-dev")
    r.add_argument("--model", default="Qwen/Qwen3-0.6B")
    r.add_argument("--vllm-image", default="docker.io/vllm/vllm-openai:v0.31.0")
    r.add_argument("--pvc-size", default="100Gi")
    r.add_argument("--duration", type=int, default=1200, help="load duration in seconds")
    r.add_argument("--offload-block-tokens", type=int, default=16)
    r.add_argument(
        "--evictor-values",
        default="{}",
        help='JSON merged into the chart values, e.g. \'{"config": {"deletionMaxFilesPerSecond": 200}}\'',
    )
    r.add_argument("--keep", action="store_true", help="leave resources running after collecting")
    r.add_argument(
        "--placement",
        choices=["any", "colocated", "separate"],
        default="any",
        help="schedule the evictor on vLLM's node, off it, or anywhere",
    )
    r.add_argument(
        "--share-nfs-client",
        action="store_true",
        help="mount like shared-vast (no nosharecache): co-located pods share one NFS client and counters merge",
    )

    r.add_argument("--gpu-memory-utilization", type=float, default=0.3)
    r.add_argument("--cpu-tier-gib", type=float, default=2.0, help="vLLM CPU offload tier size in GiB")
    r.add_argument("--tensor-parallel-size", type=int, default=1, help="vLLM TP size; requests that many GPUs")
    r.add_argument("--hf-pvc", default="kvreap-bench-hf", help="PVC holding the HF model cache (created if missing)")
    r.add_argument("--hf-pvc-size", default="50Gi", help="size when --hf-pvc has to be created")
    r.add_argument(
        "--kv-events",
        action="store_true",
        help=f"run vLLM with --kv-events-config (ZMQ PUB on :{KV_EVENTS_PORT}) and point the evictor's "
        "KV_EVENTS_ENDPOINTS at it",
    )
    r.add_argument(
        "--evictor-env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="extra env var on the evictor container, repeatable, e.g. CHAIN_EVICTION=observe",
    )

    a = sub.add_parser("analyze", help="summarize and compare run directories")
    a.add_argument("dirs", type=Path, nargs="+")

    rep = sub.add_parser("report", help="write a Markdown report with SVG charts comparing run directories")
    rep.add_argument("dirs", type=Path, nargs="+")
    rep.add_argument("--out", type=Path, required=True, help="report directory (README.md and charts/)")
    rep.add_argument("--title", default="PVC evictor benchmark")
    rep.add_argument("--notes", type=Path, help="Markdown inserted after the run table, e.g. findings")

    args = p.parse_args()
    if args.cmd == "analyze":
        return analyze.main(args.dirs)
    if args.cmd == "report":
        return chart.main(args.dirs, args.out, args.title, args.notes)

    if args.variant != "none" and (args.image is None or args.chart is None):
        p.error("--image and --chart are required unless --variant none")
    evictor_env: dict[str, str] = {}
    for kv in args.evictor_env:
        key, sep, value = kv.partition("=")
        if not sep or not key:
            p.error(f"--evictor-env takes KEY=VALUE, got {kv!r}")
        evictor_env[key] = value
    cfg = RunConfig(
        variant=args.variant,
        out=args.out,
        evictor_image=args.image,
        chart=args.chart,
        cluster=Cluster(args.context, args.namespace),
        model=args.model,
        vllm_image=args.vllm_image,
        pvc_size=args.pvc_size,
        duration_s=args.duration,
        offload_block_tokens=args.offload_block_tokens,
        evictor_values=json.loads(args.evictor_values),
        placement=args.placement,
        share_nfs_client=args.share_nfs_client,
        kv_events=args.kv_events,
        tensor_parallel_size=args.tensor_parallel_size,
        gpu_memory_utilization=args.gpu_memory_utilization,
        cpu_tier_bytes=int(args.cpu_tier_gib * 1024**3),
        hf_pvc=args.hf_pvc,
        hf_pvc_size=args.hf_pvc_size,
        evictor_env=evictor_env,
        storage_class="kvreap-bench-vast-shared" if args.share_nfs_client else "kvreap-bench-vast",
    )
    run(cfg, keep=args.keep)
    return 0


if __name__ == "__main__":
    sys.exit(main())

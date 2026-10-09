import argparse
import json
import sys
from pathlib import Path

from kvbench import analyze
from kvbench.run import Cluster, RunConfig, run


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

    a = sub.add_parser("analyze", help="summarize and compare run directories")
    a.add_argument("dirs", type=Path, nargs="+")

    args = p.parse_args()
    if args.cmd == "analyze":
        return analyze.main(args.dirs)

    if args.variant != "none" and (args.image is None or args.chart is None):
        p.error("--image and --chart are required unless --variant none")
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
    )
    run(cfg, keep=args.keep)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Fill $HF_HOME with the trace corpus and tokenizer exactly as AIPerf loads them, so the image runs offline."""

import argparse

from datasets import load_dataset
from huggingface_hub import HfApi
from transformers import AutoTokenizer


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True)
    p.add_argument("--dataset-revision", required=True)
    p.add_argument("--tokenizer", required=True)
    p.add_argument("--tokenizer-revision", required=True)
    args = p.parse_args()

    api = HfApi()
    head = api.dataset_info(args.dataset).sha
    if head != args.dataset_revision:
        raise SystemExit(f"{args.dataset} main is {head}, expected {args.dataset_revision}")
    head = api.model_info(args.tokenizer).sha
    if head != args.tokenizer_revision:
        raise SystemExit(f"{args.tokenizer} main is {head}, expected {args.tokenizer_revision}")

    ds = load_dataset(args.dataset, name=None, split="train", trust_remote_code=False, streaming=False)
    AutoTokenizer.from_pretrained(args.tokenizer, revision="main")
    print(f"{args.dataset}@{args.dataset_revision}: {len(ds)} rows; {args.tokenizer}@{args.tokenizer_revision}")


if __name__ == "__main__":
    main()

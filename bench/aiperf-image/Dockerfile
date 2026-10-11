FROM docker.io/library/python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.11.16 /uv /usr/local/bin/uv

WORKDIR /opt/aiperf
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-cache
ENV PATH=/opt/aiperf/.venv/bin:$PATH \
    HF_HOME=/opt/hf

ARG DATASET=semianalysisai/cc-traces-weka-062126-256k
ARG DATASET_REVISION=8fecd2fc56694469f758f0afbbb6335ad3043740
ARG TOKENIZER=Qwen/Qwen2.5-72B-Instruct
ARG TOKENIZER_REVISION=495f39366efef23836d0cfae4fbe635880d2be31
COPY prefetch.py ./
RUN python prefetch.py --dataset "$DATASET" --dataset-revision "$DATASET_REVISION" \
      --tokenizer "$TOKENIZER" --tokenizer-revision "$TOKENIZER_REVISION" \
    && chmod -R a+rwX /opt/hf

ENV HF_HUB_OFFLINE=1 \
    HF_DATASETS_OFFLINE=1 \
    HOME=/tmp

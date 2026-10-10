"""A stand-in for vLLM with a filesystem KV offload tier, for kvreap's kind e2e test.

`serve` binds a ZMQ PUB on $PUB_PORT and a ROUTER replay socket on $REPLAY_PORT like
`--kv-events-config '{"endpoint": "tcp://*:5557", "replay_endpoint": "tcp://*:5558"}'`,
answers replay requests from a buffer of the last REPLAY_BUFFER batches, writes block files in the fs tier layout under $ROOT, and publishes vLLM 0.31 KV events:
a GPU BlockStored (with parent and full digest) for every computed chain, then a STORAGE
BlockStored per block once its file is written (fs tier `enable_kv_events`).

The traffic is a shared preamble, LIVE sessions that keep adding turns for the whole run,
and short sessions that finish after a few turns and are never used again. The live
sessions' first turns are among the oldest blocks on disk, so an evictor that deletes by
age cuts them; one that ages whole unshared tails by their last write keeps them.

`check` reads the manifest `serve` keeps and exits non-zero unless the preamble and every
block of every live session are still on disk.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

import msgspec
import zmq

ROOT = Path(os.environ.get("ROOT", "/kv-cache"))
MODEL_BASE = ROOT / "e2e-model_0123456789ab"
RANK_DIR = ROOT / "e2e-model_0123456789ab_r0"
MANIFEST = Path(os.environ.get("MANIFEST", "/tmp/fake-vllm-manifest.json"))
BLOCK_BYTES = int(os.environ.get("BLOCK_BYTES", str(64 * 1024)))
PREAMBLE_BLOCKS = int(os.environ.get("PREAMBLE_BLOCKS", "16"))
LIVE_SESSIONS = int(os.environ.get("LIVE_SESSIONS", "4"))
FIRST_TURN_BLOCKS = int(os.environ.get("FIRST_TURN_BLOCKS", "32"))
TURN_BLOCKS = int(os.environ.get("TURN_BLOCKS", "4"))
LIVE_TURN_BLOCKS = int(os.environ.get("LIVE_TURN_BLOCKS", "1"))
SHORT_SESSION_TURNS = int(os.environ.get("SHORT_SESSION_TURNS", "3"))
TICK_SECONDS = float(os.environ.get("TICK_SECONDS", "0.5"))
PUB_PORT = int(os.environ.get("PUB_PORT", "5557"))
REPLAY_PORT = int(os.environ.get("REPLAY_PORT", "5558"))
REPLAY_BUFFER = int(os.environ.get("REPLAY_BUFFER", "10000"))
END_SEQ = (-1).to_bytes(8, "big", signed=True)
TOPIC = b"kv@fake@e2e/model"


class EventBatch(msgspec.Struct, array_like=True, omit_defaults=True, gc=False):
    ts: float
    events: list[Any]
    data_parallel_rank: int | None = None


class KVCacheEvent(msgspec.Struct, omit_defaults=True, gc=False, tag=True):
    pass


class BlockStored(KVCacheEvent):
    block_hashes: list[bytes]
    parent_block_hash: bytes | None
    token_ids: list[int]
    block_size: int
    lora_id: int | None
    medium: str | None
    lora_name: str | None


def digest(parent: bytes | None, label: str) -> bytes:
    return hashlib.sha256((parent or b"") + label.encode()).digest()


def block_path(h: bytes) -> Path:
    name = h.hex()
    return RANK_DIR / name[:3] / f"{name[3:5]}_g0" / f"{name}.bin"


class FakeVllm:
    def __init__(self) -> None:
        context = zmq.Context.instance()
        self.socket = context.socket(zmq.PUB)
        self.socket.bind(f"tcp://*:{PUB_PORT}")
        self.replay = context.socket(zmq.ROUTER)
        self.replay.bind(f"tcp://*:{REPLAY_PORT}")
        self.buffer: deque[tuple[int, bytes]] = deque(maxlen=REPLAY_BUFFER)
        self.encoder = msgspec.msgpack.Encoder()
        self.seq = 0
        self.payload = os.urandom(BLOCK_BYTES)

    def publish(self, events: list[BlockStored]) -> None:
        batch = self.encoder.encode(EventBatch(ts=time.time(), events=events, data_parallel_rank=0))
        self.buffer.append((self.seq, batch))
        self.socket.send_multipart([TOPIC, self.seq.to_bytes(8, "big"), batch])
        self.seq += 1
        self.serve_replays(0)

    def serve_replays(self, timeout_ms: int) -> None:
        """Answers replay requests like vLLM's `_service_replay`, waiting up to `timeout_ms` for the first."""
        while self.replay.poll(timeout_ms):
            frames = self.replay.recv_multipart()
            if len(frames) != 3:
                print(f"invalid replay request: {frames!r}", file=sys.stderr, flush=True)
                continue
            client, _, start = frames
            start_seq = int.from_bytes(start, "big")
            for seq, batch in self.buffer:
                if seq >= start_seq:
                    self.replay.send_multipart([client, b"", TOPIC, seq.to_bytes(8, "big"), batch])
            self.replay.send_multipart([client, b"", b"", END_SEQ, b""])
            timeout_ms = 0

    def idle(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while (left := deadline - time.monotonic()) > 0:
            self.serve_replays(int(left * 1000) + 1)

    def store_chain(self, parent: bytes | None, label: str, n: int) -> list[bytes]:
        """Computes `n` blocks after `parent`, announces them, writes them, announces the writes."""
        hashes, p = [], parent
        for i in range(n):
            p = digest(p, f"{label}/{i}")
            hashes.append(p)
        stored = BlockStored(hashes, parent, [], 16, None, "GPU", None)
        self.publish([stored])
        for h in hashes:
            path = block_path(h)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".bin.tmp")
            tmp.write_bytes(self.payload)
            tmp.rename(path)
        self.publish([BlockStored([h], None, [], 0, None, "STORAGE", None) for h in hashes])
        return hashes


def serve() -> None:
    MODEL_BASE.mkdir(parents=True, exist_ok=True)
    (MODEL_BASE / "config.json").write_text(json.dumps({"model_name": "e2e/model"}))
    vllm = FakeVllm()

    preamble = vllm.store_chain(None, "preamble", PREAMBLE_BLOCKS)
    live: dict[str, list[bytes]] = {}
    for s in range(LIVE_SESSIONS):
        live[f"live-{s}"] = vllm.store_chain(preamble[-1], f"live-{s}/turn-0", FIRST_TURN_BLOCKS)

    short = 0
    turn = 0
    while True:
        turn += 1
        for name, chain in live.items():
            chain += vllm.store_chain(chain[-1], f"{name}/turn-{turn}", LIVE_TURN_BLOCKS)
        short += 1
        chain = vllm.store_chain(preamble[-1], f"short-{short}/turn-0", FIRST_TURN_BLOCKS)
        for t in range(1, SHORT_SESSION_TURNS):
            chain += vllm.store_chain(chain[-1], f"short-{short}/turn-{t}", TURN_BLOCKS)
        manifest = {
            "preamble": [h.hex() for h in preamble],
            "live": {name: [h.hex() for h in chain] for name, chain in live.items()},
            "short_sessions": short,
        }
        MANIFEST.write_text(json.dumps(manifest))
        vllm.idle(TICK_SECONDS)


def check() -> int:
    manifest = json.loads(MANIFEST.read_text())
    missing_preamble = [h for h in manifest["preamble"] if not block_path(bytes.fromhex(h)).exists()]
    missing_live = {
        name: sum(not block_path(bytes.fromhex(h)).exists() for h in chain)
        for name, chain in manifest["live"].items()
    }
    total_live = sum(len(c) for c in manifest["live"].values())
    on_disk = sum(1 for _ in RANK_DIR.rglob("*.bin"))
    print(
        json.dumps(
            {
                "short_sessions_written": manifest["short_sessions"],
                "blocks_on_disk": on_disk,
                "preamble_missing": len(missing_preamble),
                "live_blocks": total_live,
                "live_missing": missing_live,
            }
        )
    )
    return 1 if missing_preamble or any(missing_live.values()) else 0


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "serve"
    if command == "check":
        sys.exit(check())
    serve()

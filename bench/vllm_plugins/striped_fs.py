"""Out-of-tree vLLM fs offload tier that splits each load and store across the I/O pool.

vLLM 0.31's FileSystemTierManager submits a whole promotion (one file per block, often over a
thousand files) as a single pool task, so one thread reads it file by file and the other read
threads only help across requests. This tier cuts each job into stripes, one pool task each.

Load it with a secondary tier entry such as
``{"type": "StripedFileSystemTierManager", "module_path": "striped_fs", "root_dir": ...}``
and ``striped_fs.py`` on the vLLM process's PYTHONPATH.
"""

from __future__ import annotations

import functools
import threading
from typing import Any

from vllm.v1.kv_offload.tiering.fs.io import batch_load_block, batch_store_block
from vllm.v1.kv_offload.tiering.fs.manager import FileSystemTierManager


def stripes(n_blocks: int, n_threads: int, min_blocks: int) -> list[tuple[int, int]]:
    """Contiguous `[start, end)` ranges covering `n_blocks`, at most one per thread."""
    step = max(min_blocks, -(-n_blocks // max(n_threads, 1)))
    return [(s, min(s + step, n_blocks)) for s in range(0, n_blocks, step)]


class StripedFileSystemTierManager(FileSystemTierManager):
    def __init__(
        self,
        offloading_spec: Any,
        primary_kv_view: memoryview,
        tier_type: str,
        root_dir: str,
        n_read_threads: int = 16,
        n_write_threads: int = 16,
        stripe_blocks: int = 4,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            offloading_spec,
            primary_kv_view,
            tier_type,
            root_dir,
            n_read_threads,
            n_write_threads,
            **kwargs,
        )
        self._n_read_threads = n_read_threads
        self._n_write_threads = n_write_threads
        self._stripe_blocks = max(1, stripe_blocks)
        self._progress_lock = threading.Lock()

    def _paths_and_offsets(self, job_metadata: Any) -> tuple[list[Any], list[str], list[int]]:
        keys = list(job_metadata.keys)
        paths = [self.file_mapper.get_file_name(key) for key in keys]
        offsets = [int(cid) * self._block_size for cid in job_metadata.chunk_ids]
        return keys, paths, offsets

    def submit_store(self, job_metadata: Any) -> None:
        keys, paths, offsets = self._paths_and_offsets(job_metadata)
        if not paths:
            # A job with no tasks would never complete; the stock path enqueues one.
            super().submit_store(job_metadata)
            return
        if self.events is not None:
            self._store_job_keys[job_metadata.job_id] = keys
        tasks = [
            functools.partial(
                batch_store_block,
                paths[a:b],
                self._primary_kv_view,
                offsets[a:b],
                self._block_size,
                self._use_o_direct,
            )
            for a, b in stripes(len(paths), self._n_write_threads, self._stripe_blocks)
        ]
        self._job_block_counts[job_metadata.job_id] = len(keys)
        self._pool.enqueue_store(job_metadata.job_id, len(tasks), tasks)

    def submit_load(self, job_metadata: Any) -> None:
        job_id = job_metadata.job_id
        keys, paths, offsets = self._paths_and_offsets(job_metadata)
        if not paths:
            super().submit_load(job_metadata)
            return
        self._load_job_keys[job_id] = keys
        self._job_block_counts[job_id] = len(keys)

        def load(start: int, end: int) -> None:
            try:
                batch_load_block(
                    paths[start:end],
                    self._primary_kv_view,
                    offsets[start:end],
                    self._block_size,
                    self._use_o_direct,
                )
            except OSError as exc:
                # The base class keeps blocks before the first failure; with stripes that is
                # the lowest failing index across them. Written before task_done, as upstream.
                failed_at = start + getattr(exc, "num_succeeded", 0)
                with self._progress_lock:
                    self._load_progress[job_id] = min(self._load_progress.get(job_id, len(paths)), failed_at)
                raise

        tasks = [
            functools.partial(load, a, b) for a, b in stripes(len(paths), self._n_read_threads, self._stripe_blocks)
        ]
        self._pool.enqueue_load(job_id, len(tasks), tasks)

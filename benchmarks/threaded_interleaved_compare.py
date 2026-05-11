"""Interleaved benchmark for overlapping writer and reader pressure.

This script measures the boundary where file-based drivers become less suitable
than the SQLite driver, without requiring third-party dependencies for the
historical backup import path.
"""

from __future__ import annotations

import importlib.util
import json
import math
import tempfile
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
THREAD_COUNT = 300
CONSUMER_THREADS = 64
FILES_PER_THREAD = 30
SHARED_FILES_PER_THREAD = 10
UNIQUE_FILES_PER_THREAD = FILES_PER_THREAD - SHARED_FILES_PER_THREAD
ROWS_PER_FILE = 300
EXPECTED_TOTAL_ROWS = THREAD_COUNT * FILES_PER_THREAD * ROWS_PER_FILE
EXPECTED_UNIQUE_FILES = THREAD_COUNT * UNIQUE_FILES_PER_THREAD + SHARED_FILES_PER_THREAD
EXPECTED_ROW_SUM = sum(range(ROWS_PER_FILE))
EXPECTED_ROW_SQ_SUM = sum(index * index for index in range(ROWS_PER_FILE))


def load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


CURRENT = load_module("pistributer_current_interleaved", ROOT / "pistributer.py")
BACKUP = load_module("pistributer_backup_interleaved", ROOT / "benchmarks" / "pistributer_bak.py")


def make_payload(thread_index: int, file_key: str, row_index: int) -> str:
    return json.dumps(
        {
            "thread": thread_index,
            "file": file_key,
            "row": row_index,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def build_target_paths(base_dir: Path, extension: str) -> tuple[list[list[tuple[str, Path]]], list[tuple[str, Path]]]:
    shared_targets = [
        (f"shared_{shared_index:02d}", base_dir / f"shared_{shared_index:02d}{extension}")
        for shared_index in range(SHARED_FILES_PER_THREAD)
    ]
    per_thread_targets: list[list[tuple[str, Path]]] = []

    for thread_index in range(THREAD_COUNT):
        unique_targets = [
            (
                f"thread_{thread_index:03d}_unique_{unique_index:02d}",
                base_dir / f"thread_{thread_index:03d}_unique_{unique_index:02d}{extension}",
            )
            for unique_index in range(UNIQUE_FILES_PER_THREAD)
        ]
        per_thread_targets.append(shared_targets + unique_targets)

    all_targets = shared_targets[:]
    for targets in per_thread_targets:
        all_targets.extend(targets[SHARED_FILES_PER_THREAD:])

    return per_thread_targets, all_targets


def writer_worker(module, thread_index: int, targets: list[tuple[str, Path]], start_barrier: threading.Barrier, errors: list[str]) -> None:
    try:
        start_barrier.wait()
        for file_key, channel_path in targets:
            channel_name = str(channel_path)
            for row_index in range(ROWS_PER_FILE):
                module.Pistributer.put(channel_name, make_payload(thread_index, file_key, row_index))
    except Exception as exc:
        errors.append(f"thread-{thread_index}: {exc}")


def drain_queue_once(queue):
    drained = []
    while True:
        try:
            drained.append(queue.next())
        except (StopIteration, IndexError):
            break
    return drained


def queue_is_empty(queue) -> bool:
    try:
        return bool(queue.isEmpty())
    except Exception:
        return True


def merge_stats(raw: str, group_stats: dict[str, dict[str, int]]) -> bool:
    try:
        payload = json.loads(raw)
        row_index = int(payload["row"])
        thread_index = int(payload["thread"])
        payload_file_key = str(payload["file"])
        group_key = f"{payload_file_key}::thread::{thread_index}" if payload_file_key.startswith("shared_") else payload_file_key
        stats = group_stats[group_key]
        stats["count"] += 1
        stats["row_sum"] += row_index
        stats["row_sq_sum"] += row_index * row_index
        return True
    except Exception:
        return False


def consumer_worker(module, assigned_targets, writers_done: threading.Event, results: list[dict[str, object]], slot: int) -> None:
    group_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "row_sum": 0, "row_sq_sum": 0})
    file_counts: Counter[str] = Counter()
    malformed_rows = 0
    consumed = 0
    pending = list(assigned_targets)
    queues = {file_key: module.Pistributer(str(channel_path)) for file_key, channel_path in assigned_targets}

    while pending:
        progress = False
        for file_key, channel_path in list(pending):
            queue = queues[file_key]
            drained_rows = drain_queue_once(queue)
            if drained_rows:
                progress = True
                for raw in drained_rows:
                    consumed += 1
                    file_counts[file_key] += 1
                    if not merge_stats(raw, group_stats):
                        malformed_rows += 1

            if writers_done.is_set() and queue_is_empty(queue):
                pending.remove((file_key, channel_path))

        if not progress:
            if writers_done.is_set():
                time.sleep(0.001)
            else:
                time.sleep(0.001)

    results[slot] = {
        "consumed": consumed,
        "file_counts": file_counts,
        "group_stats": group_stats,
        "malformed_rows": malformed_rows,
    }


def benchmark_version(label: str, module, extension: str) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix=f"pistributer-interleaved-{label}-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        per_thread_targets, all_targets = build_target_paths(temp_dir, extension)

        writer_start_barrier = threading.Barrier(THREAD_COUNT)
        writers_done = threading.Event()
        writer_errors: list[str] = []
        writer_threads = [
            threading.Thread(
                target=writer_worker,
                args=(module, thread_index, per_thread_targets[thread_index], writer_start_barrier, writer_errors),
                name=f"writer-{thread_index:03d}",
            )
            for thread_index in range(THREAD_COUNT)
        ]

        chunk_size = math.ceil(len(all_targets) / CONSUMER_THREADS)
        consumer_results: list[dict[str, object] | None] = [None] * CONSUMER_THREADS
        consumer_threads = []
        for worker_index in range(CONSUMER_THREADS):
            start = worker_index * chunk_size
            end = min(len(all_targets), start + chunk_size)
            assigned = all_targets[start:end]
            if not assigned:
                consumer_results[worker_index] = {"consumed": 0, "file_counts": Counter(), "group_stats": {}, "malformed_rows": 0}
                continue
            consumer_threads.append(
                threading.Thread(
                    target=consumer_worker,
                    args=(module, assigned, writers_done, consumer_results, worker_index),
                    name=f"consumer-{worker_index:03d}",
                )
            )

        start_time = time.perf_counter()
        for thread in consumer_threads:
            thread.start()
        for thread in writer_threads:
            thread.start()
        for thread in writer_threads:
            thread.join()
        writers_done.set()
        write_seconds = time.perf_counter() - start_time

        if writer_errors:
            raise RuntimeError("; ".join(writer_errors[:5]))

        for thread in consumer_threads:
            thread.join()
        total_seconds = time.perf_counter() - start_time
        read_overlap_seconds = total_seconds - write_seconds

        consumed_rows = 0
        malformed_rows = 0
        file_counts: Counter[str] = Counter()
        group_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "row_sum": 0, "row_sq_sum": 0})
        for result in consumer_results:
            if not result:
                continue
            consumed_rows += int(result["consumed"])
            malformed_rows += int(result["malformed_rows"])
            file_counts.update(result["file_counts"])
            for key, stats in result["group_stats"].items():
                group = group_stats[key]
                group["count"] += stats["count"]
                group["row_sum"] += stats["row_sum"]
                group["row_sq_sum"] += stats["row_sq_sum"]

        expected_shared_rows = THREAD_COUNT * ROWS_PER_FILE
        expected_unique_rows = ROWS_PER_FILE
        shared_file_counts_ok = all(file_counts[f"shared_{idx:02d}"] == expected_shared_rows for idx in range(SHARED_FILES_PER_THREAD))
        unique_file_counts_ok = True
        for thread_index in range(THREAD_COUNT):
            for unique_index in range(UNIQUE_FILES_PER_THREAD):
                key = f"thread_{thread_index:03d}_unique_{unique_index:02d}"
                if file_counts[key] != expected_unique_rows:
                    unique_file_counts_ok = False
                    break
            if not unique_file_counts_ok:
                break

        shared_group_integrity_ok = True
        for shared_index in range(SHARED_FILES_PER_THREAD):
            for thread_index in range(THREAD_COUNT):
                key = f"shared_{shared_index:02d}::thread::{thread_index}"
                stats = group_stats[key]
                if stats["count"] != ROWS_PER_FILE or stats["row_sum"] != EXPECTED_ROW_SUM or stats["row_sq_sum"] != EXPECTED_ROW_SQ_SUM:
                    shared_group_integrity_ok = False
                    break
            if not shared_group_integrity_ok:
                break

        unique_group_integrity_ok = True
        for thread_index in range(THREAD_COUNT):
            for unique_index in range(UNIQUE_FILES_PER_THREAD):
                key = f"thread_{thread_index:03d}_unique_{unique_index:02d}"
                stats = group_stats[key]
                if stats["count"] != ROWS_PER_FILE or stats["row_sum"] != EXPECTED_ROW_SUM or stats["row_sq_sum"] != EXPECTED_ROW_SQ_SUM:
                    unique_group_integrity_ok = False
                    break
            if not unique_group_integrity_ok:
                break

        return {
            "label": label,
            "threads": THREAD_COUNT,
            "consumer_threads": CONSUMER_THREADS,
            "files_per_thread": FILES_PER_THREAD,
            "shared_files_per_thread": SHARED_FILES_PER_THREAD,
            "rows_per_file": ROWS_PER_FILE,
            "logical_file_assignments": THREAD_COUNT * FILES_PER_THREAD,
            "physical_unique_files": EXPECTED_UNIQUE_FILES,
            "rows_total_expected": EXPECTED_TOTAL_ROWS,
            "rows_total_written_attempted": EXPECTED_TOTAL_ROWS,
            "rows_total_consumed": consumed_rows,
            "write_phase_seconds": write_seconds,
            "overlapped_read_seconds": read_overlap_seconds,
            "total_seconds": total_seconds,
            "shared_file_counts_ok": shared_file_counts_ok,
            "unique_file_counts_ok": unique_file_counts_ok,
            "malformed_rows": malformed_rows,
            "shared_group_integrity_ok": shared_group_integrity_ok,
            "unique_group_integrity_ok": unique_group_integrity_ok,
        }


def main() -> None:
    results = [
        benchmark_version("backup_txt_interleaved", BACKUP, ".txt"),
        benchmark_version("current_jsonl_interleaved", CURRENT, ".jsonl"),
    ]

    for result in results:
        print(json.dumps(result, ensure_ascii=False))

    backup = results[0]
    current = results[1]
    comparison = {
        "write_ratio_current_vs_backup": current["write_phase_seconds"] / backup["write_phase_seconds"] if backup["write_phase_seconds"] else None,
        "total_ratio_current_vs_backup": current["total_seconds"] / backup["total_seconds"] if backup["total_seconds"] else None,
    }
    print(json.dumps(comparison, ensure_ascii=False))


if __name__ == "__main__":
    main()

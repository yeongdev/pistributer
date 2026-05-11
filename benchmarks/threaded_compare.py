"""Threaded staged benchmark for the current and backup file drivers.

This benchmark stresses append throughput and full-drain integrity after the
write phase, while keeping the historical backup import path free of third-party
dependency stubs.
"""

from __future__ import annotations

import importlib.util
import json
import tempfile
import threading
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
THREAD_COUNT = 300
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


CURRENT = load_module("pistributer_current_threaded", ROOT / "pistributer.py")
BACKUP = load_module("pistributer_backup_threaded", ROOT / "benchmarks" / "pistributer_bak.py")


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


def count_written_lines(all_targets: list[tuple[str, Path]]) -> int:
	total_lines = 0
	for _, channel_path in all_targets:
		with channel_path.open("r", encoding="utf-8") as handle:
			for line in handle:
				if line.rstrip("\n"):
					total_lines += 1
	return total_lines


def consume_all(module, all_targets: list[tuple[str, Path]]) -> tuple[int, Counter, dict[str, object]]:
	consumed = 0
	file_counts: Counter[str] = Counter()
	group_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "row_sum": 0, "row_sq_sum": 0})
	malformed_rows = 0

	for file_key, channel_path in all_targets:
		queue = module.Pistributer(str(channel_path))
		while True:
			try:
				raw = queue.next()
				consumed += 1
				file_counts[file_key] += 1
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
				except Exception:
					malformed_rows += 1
			except (StopIteration, IndexError):
				break

	shared_groups_ok = True
	for shared_index in range(SHARED_FILES_PER_THREAD):
		for thread_index in range(THREAD_COUNT):
			group_key = f"shared_{shared_index:02d}::thread::{thread_index}"
			stats = group_stats[group_key]
			if (
				stats["count"] != ROWS_PER_FILE
				or stats["row_sum"] != EXPECTED_ROW_SUM
				or stats["row_sq_sum"] != EXPECTED_ROW_SQ_SUM
			):
				shared_groups_ok = False
				break
		if not shared_groups_ok:
			break

	unique_groups_ok = True
	for thread_index in range(THREAD_COUNT):
		for unique_index in range(UNIQUE_FILES_PER_THREAD):
			group_key = f"thread_{thread_index:03d}_unique_{unique_index:02d}"
			stats = group_stats[group_key]
			if (
				stats["count"] != ROWS_PER_FILE
				or stats["row_sum"] != EXPECTED_ROW_SUM
				or stats["row_sq_sum"] != EXPECTED_ROW_SQ_SUM
			):
				unique_groups_ok = False
				break
		if not unique_groups_ok:
			break

	integrity = {
		"malformed_rows": malformed_rows,
		"shared_groups_ok": shared_groups_ok,
		"unique_groups_ok": unique_groups_ok,
	}

	return consumed, file_counts, integrity


def benchmark_version(label: str, module, extension: str) -> dict[str, object]:
	with tempfile.TemporaryDirectory(prefix=f"pistributer-threaded-{label}-") as temp_dir_name:
		temp_dir = Path(temp_dir_name)
		per_thread_targets, all_targets = build_target_paths(temp_dir, extension)

		start_barrier = threading.Barrier(THREAD_COUNT)
		writer_errors: list[str] = []
		threads = [
			threading.Thread(
				target=writer_worker,
				args=(module, thread_index, per_thread_targets[thread_index], start_barrier, writer_errors),
				name=f"writer-{thread_index:03d}",
			)
			for thread_index in range(THREAD_COUNT)
		]

		write_start = time.perf_counter()
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		write_seconds = time.perf_counter() - write_start

		if writer_errors:
			raise RuntimeError("; ".join(writer_errors[:5]))

		written_rows = count_written_lines(all_targets)

		read_start = time.perf_counter()
		consumed_rows, file_counts, integrity = consume_all(module, all_targets)
		read_seconds = time.perf_counter() - read_start

		expected_shared_rows = THREAD_COUNT * ROWS_PER_FILE
		expected_unique_rows = ROWS_PER_FILE
		shared_ok = all(file_counts[f"shared_{idx:02d}"] == expected_shared_rows for idx in range(SHARED_FILES_PER_THREAD))
		unique_ok = True
		for thread_index in range(THREAD_COUNT):
			for unique_index in range(UNIQUE_FILES_PER_THREAD):
				key = f"thread_{thread_index:03d}_unique_{unique_index:02d}"
				if file_counts[key] != expected_unique_rows:
					unique_ok = False
					break
			if not unique_ok:
				break

		return {
			"label": label,
			"threads": THREAD_COUNT,
			"files_per_thread": FILES_PER_THREAD,
			"shared_files_per_thread": SHARED_FILES_PER_THREAD,
			"rows_per_file": ROWS_PER_FILE,
			"logical_file_assignments": THREAD_COUNT * FILES_PER_THREAD,
			"physical_unique_files": EXPECTED_UNIQUE_FILES,
			"rows_total_expected": EXPECTED_TOTAL_ROWS,
			"rows_total_written": written_rows,
			"rows_total_consumed": consumed_rows,
			"write_seconds": write_seconds,
			"read_seconds": read_seconds,
			"total_seconds": write_seconds + read_seconds,
			"shared_file_counts_ok": shared_ok,
			"unique_file_counts_ok": unique_ok,
			"malformed_rows": integrity["malformed_rows"],
			"shared_group_integrity_ok": integrity["shared_groups_ok"],
			"unique_group_integrity_ok": integrity["unique_groups_ok"],
		}


def main() -> None:
	results = [
		benchmark_version("backup_txt_threaded", BACKUP, ".txt"),
		benchmark_version("current_jsonl_threaded", CURRENT, ".jsonl"),
	]

	for result in results:
		print(json.dumps(result, ensure_ascii=False))

	backup = results[0]
	current = results[1]
	comparison = {
		"write_ratio_current_vs_backup": current["write_seconds"] / backup["write_seconds"] if backup["write_seconds"] else None,
		"read_ratio_current_vs_backup": current["read_seconds"] / backup["read_seconds"] if backup["read_seconds"] else None,
		"total_ratio_current_vs_backup": current["total_seconds"] / backup["total_seconds"] if backup["total_seconds"] else None,
	}
	print(json.dumps(comparison, ensure_ascii=False))


if __name__ == "__main__":
	main()

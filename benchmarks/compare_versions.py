"""Staged benchmark for the current and backup file drivers.

This script compares local write-then-consume performance without requiring any
third-party dependency just to import the historical backup implementation.
"""

from __future__ import annotations

import importlib.util
import json
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FILE_COUNT = 300
LINES_PER_FILE = 30


def load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


CURRENT = load_module("pistributer_current", ROOT / "pistributer.py")
BACKUP = load_module("pistributer_backup", ROOT / "benchmarks" / "pistributer_bak.py")


def make_payload(file_index: int, row_index: int) -> str:
    return json.dumps(
        {
            "file": file_index,
            "row": row_index,
            "payload": f"payload-{file_index}-{row_index}",
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def benchmark_version(label: str, module, extension: str) -> dict[str, float | int | str]:
    with tempfile.TemporaryDirectory(prefix=f"pistributer-{label}-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        paths = [temp_dir / f"channel_{index:03d}{extension}" for index in range(FILE_COUNT)]

        write_start = time.perf_counter()
        for file_index, channel_path in enumerate(paths):
            for row_index in range(LINES_PER_FILE):
                module.Pistributer.put(str(channel_path), make_payload(file_index, row_index))
        write_seconds = time.perf_counter() - write_start

        read_start = time.perf_counter()
        consumed = 0
        for channel_path in paths:
            queue = module.Pistributer(str(channel_path))
            for _ in range(LINES_PER_FILE):
                queue.next()
                consumed += 1
            queue.isEmpty()
        read_seconds = time.perf_counter() - read_start

        return {
            "label": label,
            "files": FILE_COUNT,
            "rows_per_file": LINES_PER_FILE,
            "rows_total": consumed,
            "write_seconds": write_seconds,
            "read_seconds": read_seconds,
            "total_seconds": write_seconds + read_seconds,
        }


def main() -> None:
    results = [
        benchmark_version("backup_txt", BACKUP, ".txt"),
        benchmark_version("current_jsonl", CURRENT, ".jsonl"),
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

"""JSONL-first `pistributer` driver.

This module is the default public delivery of `pistributer`.

It keeps the original local file-queue model: append records to a data file,
rotate that file into `.in_use`, and track read progress in a sidecar index.

Use this driver when you want structured payloads and a small local queue API.
The file driver is best for staged workflows or single-writer-friendly usage.
It is not the strongest choice for heavy overlapping write and read contention.
The hot write path assumes the parent directory already exists.

Example:
    >>> from pistributer import Pistributer
    >>> Pistributer.put("events.jsonl", {"event": "start"})
    True
    >>> queue = Pistributer("events.jsonl")
    >>> isinstance(queue.next(), str)
    True
"""

from __future__ import annotations

import json
import os
from pathlib import Path

__all__ = ["Pistributer"]
__version__ = "0.2.0"


class Pistributer:
    """Queue interface for newline-delimited JSON files.

    The public method names intentionally preserve the historical API, including
    `isEmpty()` and `getIndex()`, so existing code keeps working.

    Example:
        >>> queue = Pistributer("events.jsonl")
        >>> queue.isEmpty()
        True
    """

    def __init__(self, path: str | Path):
        """Open a queue backed by a `.jsonl` channel file.

        Args:
            path: Path to the queue file. The path must end with `.jsonl`.

        Returns:
            None.

        Raises:
            ValueError: If `path` does not end with `.jsonl`.

        Example:
            >>> Pistributer("events.jsonl")
        """
        channel_path = self._normalize_channel_name(path)
        self.abspath = "" if os.sep not in channel_path else os.path.abspath(".")
        self.path = {
            "data": os.path.abspath(os.path.join(self.abspath, channel_path)),
            "index": os.path.abspath(os.path.join(self.abspath, f"{channel_path}.index")),
            "in_use": os.path.abspath(os.path.join(self.abspath, f"{channel_path}.in_use")),
        }
        self.q: list[str] = []
        self.__index = {"index": 0}
        self.__initialQueue()

    @staticmethod
    def new(target_path: str | Path, string, overwrite: bool = False, sep: str = "") -> bool:
        """Create a new queue file with one serialized record.

        Args:
            target_path: Destination `.jsonl` file.
            string: Record to write. Strings are written as-is; other values are
                serialized to compact JSON.
            overwrite: When `True`, allow replacing an existing file.
            sep: Optional trailing separator to append after the payload.

        Returns:
            True when the file is created successfully.

        Raises:
            ValueError: If `target_path` does not end with `.jsonl`.
            IsADirectoryError: If `target_path` points to a directory.
            FileExistsError: If the file already exists and `overwrite` is `False`.

        Example:
            >>> Pistributer.new("events.jsonl", {"event": "start"}, overwrite=True)
            True
        """
        file_path = Pistributer._normalize_channel_name(target_path)
        if os.path.isdir(file_path):
            raise IsADirectoryError(f'This is a folder, require a file to write "{file_path}"')
        if os.path.isfile(file_path) and overwrite is not True:
            raise FileExistsError(f'File exists "{file_path}"')

        payload = Pistributer._serialize_record(string)
        parent_dir = os.path.dirname(file_path)
        if parent_dir and not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

        with open(file_path, "w+", encoding="utf-8") as handle:
            handle.write(f"{payload}{sep}")
            handle.flush()

        return True

    @staticmethod
    def put(target_path: str | Path, string) -> bool:
        """Append one record to a `.jsonl` queue file.

        Args:
            target_path: Destination `.jsonl` file.
            string: Record to append. Strings are written as-is; other values are
                serialized to compact JSON.

        Returns:
            True when the record is appended successfully.

        Raises:
            ValueError: If `target_path` does not end with `.jsonl`.
            FileNotFoundError: If the parent directory does not already exist.

        Example:
            >>> Pistributer.put("events.jsonl", {"event": "finish"})
            True
        """
        file_path = Pistributer._normalize_channel_name(target_path)
        payload = Pistributer._serialize_record(string)

        with open(file_path, "a+", encoding="utf-8") as handle:
            handle.write(f"{payload}\n")
            handle.flush()

        return True

    def next(self) -> str:
        """Return the next unread record.

        Returns:
            The next stored line as a string.

        Raises:
            StopIteration: If the queue is empty.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> queue.next()
        """
        if self.isEmpty():
            raise StopIteration("Pistributer queue is empty")

        data = self.q[self.__index["index"]]
        self.increaseIndex()
        return data

    def isEmpty(self) -> bool:
        """Report whether the queue has unread records.

        This historical camelCase name is kept for compatibility.

        Returns:
            True when no unread records remain, otherwise False.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> queue.isEmpty()
            True
        """
        if self.__index["index"] >= len(self.q):
            if os.path.isfile(self.path["index"]):
                os.remove(self.path["index"])
            if os.path.isfile(self.path["in_use"]):
                os.remove(self.path["in_use"])

            if os.path.isfile(self.path["data"]):
                self.__initialQueue()
                return False

            return True

        return False

    def size(self) -> int:
        """Count records across the active data and `.in_use` files.

        Returns:
            The total number of stored records for the channel.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> isinstance(queue.size(), int)
            True
        """
        paths = self.path["data"], self.path["in_use"]
        size = 0
        for file_path in paths:
            if os.path.isfile(file_path):
                with open(file_path, encoding="utf-8") as handle:
                    for _ in handle:
                        size += 1
        return size

    def remaining(self) -> int:
        """Count unread records.

        Returns:
            The number of records that have not been consumed yet.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> isinstance(queue.remaining(), int)
            True
        """
        return max(0, self.size() - self.getIndex()["index"])

    def getIndex(self) -> dict[str, int]:
        """Return the persisted read index.

        Returns:
            A dictionary with one key, `index`, storing the consumed count.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> "index" in queue.getIndex()
            True
        """
        if os.path.isfile(self.path["index"]):
            with open(self.path["index"], "r+", encoding="utf-8") as json_file:
                try:
                    output = json.load(json_file)
                except Exception:
                    return {"index": 0}
            return output

        with open(self.path["index"], "w+", encoding="utf-8") as handle:
            json.dump({"index": 0}, handle)

        return {"index": 0}

    def updateIndex(self, count: int) -> None:
        """Persist a new read index value.

        Args:
            count: Desired consumed count. Negative values are clamped to zero.

        Returns:
            None.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> queue.updateIndex(0)
        """
        self.__index["index"] = max(0, count)
        with open(self.path["index"], "w+", encoding="utf-8") as handle:
            json.dump(self.__index, handle)

    def increaseIndex(self, count: int = 1) -> None:
        """Advance the persisted read index.

        Args:
            count: Number of consumed records to add.

        Returns:
            None.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> queue.increaseIndex()
        """
        self.updateIndex(self.__index["index"] + count)

    def decreaseIndex(self, count: int = 1) -> None:
        """Move the persisted read index backward.

        Args:
            count: Number of consumed records to subtract.

        Returns:
            None.

        Example:
            >>> queue = Pistributer("events.jsonl")
            >>> queue.decreaseIndex()
        """
        self.updateIndex(self.__index["index"] - count)

    def __initialQueue(self) -> None:
        self.__index = self.getIndex()

        if os.path.isfile(self.path["in_use"]):
            self.q = list(self.__readInUse())
            return

        if os.path.isfile(self.path["data"]):
            os.rename(self.path["data"], self.path["in_use"])
            self.q = list(self.__readInUse())
            self.updateIndex(0)
            return

        self.q = []
        if os.path.isfile(self.path["index"]):
            os.remove(self.path["index"])

    def __readInUse(self):
        if os.path.isfile(self.path["in_use"]):
            return [item for item in self.__read_file_by_line(self.path["in_use"]) if item != ""]
        return []

    def __read_file_by_line(self, file_path: str):
        if os.path.isfile(file_path):
            with open(file_path, "r", encoding="utf-8") as handle:
                return handle.read().split("\n")
        return []

    @staticmethod
    def _normalize_channel_name(path: str | Path) -> str:
        """Validate a `.jsonl` channel path.

        Args:
            path: Candidate file path.

        Returns:
            The normalized filesystem path string.

        Raises:
            ValueError: If the file name does not end with `.jsonl`.
        """
        file_path = os.fspath(path)
        if not file_path.endswith(".jsonl"):
            raise ValueError(f'Path must end with ".jsonl" > "{file_path}"')
        return file_path

    @staticmethod
    def _serialize_record(record) -> str:
        """Convert a record to the line format stored by this driver.

        Args:
            record: String or JSON-serializable value.

        Returns:
            A single-line string ready to append to the queue file.
        """
        if isinstance(record, str):
            return record
        return json.dumps(record, ensure_ascii=False, separators=(",", ":"))
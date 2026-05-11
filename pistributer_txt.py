"""Text-first `pistributer` driver.

This module preserves the shortest file-backed queue path in the project.

Use it when you want raw text throughput and simple append-heavy local queueing.
Like the JSONL file driver, it is best for staged workflows or
single-writer-friendly usage. It is not designed for strong overlapping
writer and reader integrity.
The hot write path assumes the parent directory already exists.

Example:
    >>> from pistributer_txt import PistributerTxt
    >>> PistributerTxt.put("events.txt", "start")
    True
"""

from __future__ import annotations

import json
import os
from pathlib import Path

__all__ = ["PistributerTxt"]


class PistributerTxt:
    """Queue interface for plain-text channel files.

    The public method names intentionally preserve the historical file-driver
    API, including `isEmpty()` and `getIndex()`.
    """

    def __init__(self, path: str | Path):
        """Open a queue backed by a `.txt` file.

        Args:
            path: Path to the queue file. The path must end with `.txt`.

        Returns:
            None.

        Raises:
            ValueError: If `path` does not end with `.txt`.

        Example:
            >>> PistributerTxt("events.txt")
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
        self.__initial_queue()

    @staticmethod
    def new(target_path: str | Path, string, overwrite: bool = False, sep: str = "") -> bool:
        """Create a new `.txt` queue file with one initial line.

        Args:
            target_path: Destination `.txt` file.
            string: Text payload to write.
            overwrite: When `True`, allow replacing an existing file.
            sep: Optional trailing separator to append.

        Returns:
            True when the file is created successfully.

        Raises:
            ValueError: If `target_path` does not end with `.txt`.
            IsADirectoryError: If `target_path` points to a directory.
            FileExistsError: If the file already exists and `overwrite` is `False`.

        Example:
            >>> PistributerTxt.new("events.txt", "start", overwrite=True)
            True
        """
        file_path = PistributerTxt._normalize_channel_name(target_path)
        if os.path.isdir(file_path):
            raise IsADirectoryError(f'This is a folder, require a file to write "{file_path}"')
        if os.path.isfile(file_path) and overwrite is not True:
            raise FileExistsError(f'File exists "{file_path}"')

        parent_dir = os.path.dirname(file_path)
        if parent_dir and not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

        with open(file_path, "w+", encoding="utf-8") as handle:
            handle.write(f"{string}{sep}")
            handle.flush()
        return True

    @staticmethod
    def put(target_path: str | Path, string) -> bool:
        """Append one text line to a `.txt` queue file.

        Args:
            target_path: Destination `.txt` file.
            string: Text payload to append.

        Returns:
            True when the line is appended successfully.

        Raises:
            ValueError: If `target_path` does not end with `.txt`.
            FileNotFoundError: If the parent directory does not already exist.

        Example:
            >>> PistributerTxt.put("events.txt", "finish")
            True
        """
        file_path = PistributerTxt._normalize_channel_name(target_path)

        with open(file_path, "a+", encoding="utf-8") as handle:
            handle.write(f"{string}\n")
            handle.flush()
        return True

    def next(self) -> str:
        """Return the next unread text line.

        Returns:
            The next stored line.

        Raises:
            StopIteration: If the queue is empty.

        Example:
            >>> queue = PistributerTxt("events.txt")
            >>> queue.next()
        """
        if self.isEmpty():
            raise StopIteration("PistributerTxt queue is empty")
        data = self.q[self.__index["index"]]
        self.increaseIndex()
        return data

    def isEmpty(self) -> bool:
        """Report whether the queue has unread text lines.

        This historical camelCase name is kept for compatibility.

        Returns:
            True when no unread records remain, otherwise False.

        Example:
            >>> queue = PistributerTxt("events.txt")
            >>> queue.isEmpty()
            True
        """
        if self.__index["index"] >= len(self.q):
            if os.path.isfile(self.path["index"]):
                os.remove(self.path["index"])
            if os.path.isfile(self.path["in_use"]):
                os.remove(self.path["in_use"])
            if os.path.isfile(self.path["data"]):
                self.__initial_queue()
                return False
            return True
        return False

    def size(self) -> int:
        """Count records across the active data and `.in_use` files.

        Returns:
            The total number of stored text lines for the channel.
        """
        size = 0
        for file_path in (self.path["data"], self.path["in_use"]):
            if os.path.isfile(file_path):
                with open(file_path, encoding="utf-8") as handle:
                    for _ in handle:
                        size += 1
        return size

    def remaining(self) -> int:
        """Count unread text lines.

        Returns:
            The number of records that have not been consumed yet.
        """
        return max(0, self.size() - self.getIndex()["index"])

    def getIndex(self) -> dict[str, int]:
        """Return the persisted read index.

        Returns:
            A dictionary with one key, `index`, storing the consumed count.
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
        """
        self.updateIndex(self.__index["index"] + count)

    def decreaseIndex(self, count: int = 1) -> None:
        """Move the persisted read index backward.

        Args:
            count: Number of consumed records to subtract.

        Returns:
            None.
        """
        self.updateIndex(self.__index["index"] - count)

    def __initial_queue(self) -> None:
        self.__index = self.getIndex()
        if os.path.isfile(self.path["in_use"]):
            self.q = list(self.__read_in_use())
            return
        if os.path.isfile(self.path["data"]):
            os.rename(self.path["data"], self.path["in_use"])
            self.q = list(self.__read_in_use())
            self.updateIndex(0)
            return
        self.q = []
        if os.path.isfile(self.path["index"]):
            os.remove(self.path["index"])

    def __read_in_use(self):
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
        """Validate a `.txt` channel path.

        Args:
            path: Candidate file path.

        Returns:
            The normalized filesystem path string.

        Raises:
            ValueError: If the file name does not end with `.txt`.
        """
        file_path = os.fspath(path)
        if not file_path.endswith(".txt"):
            raise ValueError(f'Path must end with ".txt" > "{file_path}"')
        return file_path

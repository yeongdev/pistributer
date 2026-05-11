"""SQLite-backed `pistributer` driver.

This module is the integrity-first local queue in the project.

Use it when correctness matters more than the shortest append path.
It has a different public naming style for `is_empty()` because it was added as
the newer SQLite-specific driver, but the existing name is kept stable.

Example:
    >>> from pistributer_sqlite import PistributerSqlite
    >>> queue = PistributerSqlite("events.db")
    >>> queue.put("start")
    True
    >>> queue.close()
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

__all__ = ["PistributerSqlite"]


class PistributerSqlite:
    """Queue interface backed by a local SQLite database."""

    def __init__(self, path: str | Path):
        """Open or create a `.db` queue file.

        Args:
            path: Path to the SQLite database file. The path must end with `.db`.

        Returns:
            None.

        Raises:
            ValueError: If `path` does not end with `.db`.

        Example:
            >>> PistributerSqlite("events.db")
        """
        self.path = self._normalize_db_path(path)
        parent_dir = os.path.dirname(self.path)
        if parent_dir and not os.path.isdir(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS pistributer_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, consumed INTEGER NOT NULL DEFAULT 0)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_pistributer_queue_consumed_id ON pistributer_queue(consumed, id)"
        )

    @staticmethod
    def new(target_path: str | Path, rows, overwrite: bool = False) -> bool:
        """Create a new SQLite queue and preload rows.

        Args:
            target_path: Destination `.db` file.
            rows: Iterable of payloads to insert.
            overwrite: When `True`, allow replacing an existing database file.

        Returns:
            True when the database is created and populated successfully.

        Raises:
            ValueError: If `target_path` does not end with `.db`.
            FileExistsError: If the file already exists and `overwrite` is `False`.

        Example:
            >>> PistributerSqlite.new("events.db", ["start"], overwrite=True)
            True
        """
        db_path = PistributerSqlite._normalize_db_path(target_path)
        if os.path.exists(db_path) and not overwrite:
            raise FileExistsError(f'File exists "{db_path}"')
        if os.path.exists(db_path) and overwrite:
            os.remove(db_path)
        queue = PistributerSqlite(db_path)
        queue.put_many(rows)
        queue.close()
        return True

    def put(self, payload: str) -> bool:
        """Append one payload to the queue.

        Args:
            payload: Value to store. It is converted to string before insert.

        Returns:
            True when the insert succeeds.

        Example:
            >>> queue = PistributerSqlite("events.db")
            >>> queue.put("finish")
            True
        """
        self.connection.execute("INSERT INTO pistributer_queue (payload) VALUES (?)", (str(payload),))
        return True

    def put_many(self, payloads) -> bool:
        """Append many payloads in one transaction.

        Args:
            payloads: Iterable of values to store.

        Returns:
            True when the batch insert succeeds.

        Example:
            >>> queue = PistributerSqlite("events.db")
            >>> queue.put_many(["a", "b"])
            True
        """
        with self.connection:
            self.connection.executemany(
                "INSERT INTO pistributer_queue (payload) VALUES (?)",
                [(str(payload),) for payload in payloads],
            )
        return True

    def next(self) -> str:
        """Return the next unread payload.

        Returns:
            The next stored payload as a string.

        Raises:
            StopIteration: If the queue is empty.

        Example:
            >>> queue = PistributerSqlite("events.db")
            >>> queue.next()
        """
        with self.connection:
            row = self.connection.execute(
                "SELECT id, payload FROM pistributer_queue WHERE consumed = 0 ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                raise StopIteration("PistributerSqlite queue is empty")
            self.connection.execute("UPDATE pistributer_queue SET consumed = 1 WHERE id = ?", (row[0],))
        return row[1]

    def is_empty(self) -> bool:
        """Report whether unread payloads remain.

        Returns:
            True when no unread rows remain, otherwise False.

        Example:
            >>> queue = PistributerSqlite("events.db")
            >>> queue.is_empty()
            True
        """
        row = self.connection.execute(
            "SELECT 1 FROM pistributer_queue WHERE consumed = 0 LIMIT 1"
        ).fetchone()
        return row is None

    def size(self) -> int:
        """Count all rows in the queue table.

        Returns:
            The total number of stored rows.
        """
        row = self.connection.execute("SELECT COUNT(*) FROM pistributer_queue").fetchone()
        return int(row[0]) if row else 0

    def remaining(self) -> int:
        """Count unread rows in the queue table.

        Returns:
            The number of rows that have not been consumed yet.
        """
        row = self.connection.execute("SELECT COUNT(*) FROM pistributer_queue WHERE consumed = 0").fetchone()
        return int(row[0]) if row else 0

    def close(self) -> None:
        """Close the SQLite connection.

        Returns:
            None.

        Example:
            >>> queue = PistributerSqlite("events.db")
            >>> queue.close()
        """
        self.connection.close()

    @staticmethod
    def _normalize_db_path(path: str | Path) -> str:
        """Validate a `.db` queue path.

        Args:
            path: Candidate database file path.

        Returns:
            The normalized filesystem path string.

        Raises:
            ValueError: If the file name does not end with `.db`.
        """
        file_path = os.fspath(path)
        if not file_path.endswith(".db"):
            raise ValueError(f'Path must end with ".db" > "{file_path}"')
        return file_path

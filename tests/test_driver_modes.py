from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pistributer import Pistributer
from pistributer_sqlite import PistributerSqlite
from pistributer_txt import PistributerTxt


class DriverModeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_txt_driver_roundtrip(self) -> None:
        path = self.temp_dir / "channel.txt"
        PistributerTxt.put(path, "hello")
        PistributerTxt.put(path, "world")
        queue = PistributerTxt(path)
        self.assertEqual(queue.next(), "hello")
        self.assertEqual(queue.next(), "world")
        self.assertTrue(queue.isEmpty())

    def test_sqlite_driver_roundtrip(self) -> None:
        path = self.temp_dir / "channel.db"
        queue = PistributerSqlite(path)
        queue.put("hello")
        queue.put("world")
        self.assertEqual(queue.next(), "hello")
        self.assertEqual(queue.next(), "world")
        self.assertTrue(queue.is_empty())
        queue.close()

    def test_jsonl_driver_roundtrip(self) -> None:
        path = self.temp_dir / "channel.jsonl"
        Pistributer.put(path, {"value": "hello"})
        Pistributer.put(path, {"value": "world"})
        queue = Pistributer(path)
        self.assertEqual(json.loads(queue.next()), {"value": "hello"})
        self.assertEqual(json.loads(queue.next()), {"value": "world"})
        self.assertTrue(queue.isEmpty())

    def test_txt_put_requires_existing_parent_directory(self) -> None:
        path = self.temp_dir / "missing" / "channel.txt"

        with self.assertRaises(FileNotFoundError):
            PistributerTxt.put(path, "hello")


if __name__ == "__main__":
    unittest.main()

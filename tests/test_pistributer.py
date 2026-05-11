from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from pistributer import Pistributer


class PistributerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp())
        self.channel = self.temp_dir / "channel.jsonl"

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_fifo_read_order(self) -> None:
        Pistributer.put(self.channel, {"value": "hello"})
        Pistributer.put(self.channel, {"value": "world"})

        queue = Pistributer(self.channel)

        self.assertEqual(json.loads(queue.next()), {"value": "hello"})
        self.assertEqual(json.loads(queue.next()), {"value": "world"})
        self.assertTrue(queue.isEmpty())

    def test_next_raises_when_empty(self) -> None:
        queue = Pistributer(self.channel)

        with self.assertRaises(StopIteration):
            queue.next()

    def test_remaining_counts_unread_messages(self) -> None:
        Pistributer.put(self.channel, {"value": "one"})
        Pistributer.put(self.channel, {"value": "two"})
        Pistributer.put(self.channel, {"value": "three"})

        queue = Pistributer(self.channel)
        self.assertEqual(queue.remaining(), 3)
        self.assertEqual(json.loads(queue.next()), {"value": "one"})
        self.assertEqual(queue.remaining(), 2)

    def test_progress_survives_reopen(self) -> None:
        Pistributer.put(self.channel, {"value": "one"})
        Pistributer.put(self.channel, {"value": "two"})

        queue = Pistributer(self.channel)
        self.assertEqual(json.loads(queue.next()), {"value": "one"})

        reopened = Pistributer(self.channel)
        self.assertEqual(reopened.getIndex(), {"index": 1})
        self.assertEqual(json.loads(reopened.next()), {"value": "two"})
        self.assertTrue(reopened.isEmpty())

    def test_consumes_rotated_and_new_data(self) -> None:
        Pistributer.put(self.channel, {"value": "first"})
        Pistributer.put(self.channel, {"value": "second"})

        queue = Pistributer(self.channel)
        self.assertEqual(json.loads(queue.next()), {"value": "first"})

        Pistributer.put(self.channel, {"value": "third"})
        Pistributer.put(self.channel, {"value": "fourth"})

        self.assertEqual(json.loads(queue.next()), {"value": "second"})
        self.assertEqual(json.loads(queue.next()), {"value": "third"})
        self.assertEqual(json.loads(queue.next()), {"value": "fourth"})
        self.assertTrue(queue.isEmpty())

    def test_requires_jsonl_extension(self) -> None:
        with self.assertRaises(ValueError):
            Pistributer(self.temp_dir / "channel.txt")

        with self.assertRaises(ValueError):
            Pistributer.put(self.temp_dir / "channel.txt", {"value": "bad"})

    def test_put_requires_existing_parent_directory(self) -> None:
        missing_channel = self.temp_dir / "missing" / "channel.jsonl"

        with self.assertRaises(FileNotFoundError):
            Pistributer.put(missing_channel, {"value": "bad"})

    def test_bulk_300_jsonl_files_with_30_json_rows_each(self) -> None:
        total_rows = 0

        for file_index in range(300):
            channel = self.temp_dir / f"channel_{file_index:03d}.jsonl"
            for row_index in range(30):
                Pistributer.put(
                    channel,
                    {
                        "file": file_index,
                        "row": row_index,
                        "payload": f"payload-{file_index}-{row_index}",
                    },
                )

        for file_index in range(300):
            channel = self.temp_dir / f"channel_{file_index:03d}.jsonl"
            queue = Pistributer(channel)
            for row_index in range(30):
                item = json.loads(queue.next())
                self.assertEqual(item["file"], file_index)
                self.assertEqual(item["row"], row_index)
                self.assertEqual(item["payload"], f"payload-{file_index}-{row_index}")
                total_rows += 1
            self.assertTrue(queue.isEmpty())

        self.assertEqual(total_rows, 9000)


if __name__ == "__main__":
    unittest.main()

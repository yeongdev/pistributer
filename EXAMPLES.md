# Examples

This document shows small, copyable examples for each supported `pistributer` driver.

## Built-in help surface

The public modules and methods now include prompt-friendly docstrings, so Python's built-in help is a supported discovery path.

```python
from pistributer import Pistributer
from pistributer_txt import PistributerTxt
from pistributer_sqlite import PistributerSqlite

help(Pistributer)
help(Pistributer.put)
help(PistributerTxt.isEmpty)
help(PistributerSqlite.is_empty)
```

## `jsonl` driver

```python
from pistributer import Pistributer

channel = "events.jsonl"

Pistributer.put(channel, {"event": "start", "id": 1})
Pistributer.put(channel, {"event": "finish", "id": 1})

queue = Pistributer(channel)

print(queue.next())
print(queue.next())
print(queue.isEmpty())
```

## `txt` driver

```python
from pistributer_txt import PistributerTxt

channel = "events.txt"

PistributerTxt.put(channel, "start:1")
PistributerTxt.put(channel, "finish:1")

queue = PistributerTxt(channel)

print(queue.next())
print(queue.next())
print(queue.isEmpty())
```

## `sqlite` driver

```python
from pistributer_sqlite import PistributerSqlite

channel = "events.db"

queue = PistributerSqlite(channel)
queue.put("start:1")
queue.put("finish:1")

print(queue.next())
print(queue.next())
print(queue.is_empty())
queue.close()
```

## Failure-boundary example

The two file drivers are not the best fit when many writers and many readers overlap heavily on the same workload window.

Use `PistributerSqlite` instead of a file driver when the main requirement is stronger overlap correctness.

The two file drivers also assume that the parent directory already exists before `put()` is called.

That means this succeeds:

```python
from pathlib import Path

from pistributer import Pistributer

base = Path("queues")
base.mkdir(parents=True, exist_ok=True)

Pistributer.put(base / "events.jsonl", {"event": "start"})
```

And this fails with `FileNotFoundError` because the parent directory was not prepared first:

```python
from pistributer import Pistributer

Pistributer.put("missing/events.jsonl", {"event": "start"})
```

This is the kind of situation where the file drivers are the wrong tool:

```python
from concurrent.futures import ThreadPoolExecutor

from pistributer import Pistributer

channel = "events.jsonl"


def writer(batch_id: int) -> None:
    for row in range(1000):
        Pistributer.put(channel, {"batch": batch_id, "row": row})


def reader() -> None:
    queue = Pistributer(channel)
    while True:
        try:
            queue.next()
        except StopIteration:
            break


with ThreadPoolExecutor(max_workers=32) as executor:
    for batch_id in range(16):
        executor.submit(writer, batch_id)
    for _ in range(16):
        executor.submit(reader)
```

That pattern creates heavy overlapping write/read pressure. The file drivers are intentionally not the strongest option for that case.

Choose one of these instead:

- keep the workload staged so writes and reads are mostly separated
- keep file-driver usage single-writer-friendly
- switch to `PistributerSqlite` when overlap correctness matters more than raw file simplicity

## Test and benchmark commands

```bash
python -m unittest discover -s tests -v
python benchmarks/compare_versions.py
python benchmarks/threaded_interleaved_compare.py
```

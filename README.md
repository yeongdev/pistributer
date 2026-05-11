# Pistributer

`pistributer` is a local-first FIFO queue toolkit for file-based workflows.

If you want a queue you can install with `pip` and use immediately on local files, this project is for you.

It helps you:

- write messages into a file-backed queue
- read those messages back in FIFO order
- avoid standing up Redis, Kafka, or another separate service for small and medium local workflows

For most new users, the default `jsonl` driver is the right place to start.

## Start here if you are new

If you only want the shortest possible path from zero to working code, do this:

### 1. Install Python

Make sure you have Python `3.9+`:

```bash
python3 --version
```

If that prints a version, you are ready.

### 2. Install `pistributer`

```bash
python3 -m pip install pistributer
```

If `pip` says the package is already installed, that is fine.

### 3. Create a test script

Create a file named `demo.py` with this content:

```python
from pathlib import Path

from pistributer import Pistributer

queue_dir = Path("queues")
queue_dir.mkdir(parents=True, exist_ok=True)

queue_file = queue_dir / "demo.jsonl"

Pistributer.put(queue_file, {"message": "hello"})
Pistributer.put(queue_file, {"message": "world"})

queue = Pistributer(queue_file)

print(queue.next())
print(queue.next())
print(queue.isEmpty())
```

### 4. Run it

```bash
python3 demo.py
```

You should see two queue messages printed, then `True` after the queue becomes empty.

That is the basic workflow:

1. create a folder for your queue files
2. write messages with `put()`
3. open the queue with `Pistributer(...)`
4. read messages with `next()`

**Hard position:** `pistributer` exists for developers who want a usable queue across servers or local jobs without standing up Redis, Kafka, or another heavy service.

**Performance contract:** this project prefers faster write/read throughput over a larger or more polished interface. The core value is still the same: write, read, high concurrency, multi-file workloads, and as little extra overhead as possible.

It started as a high-throughput local queue used on large datasets, and it now ships three practical drivers instead of pretending one storage format is ideal for every workload:

- `txt` for the shortest raw file path
- `jsonl` for structured, inspectable records
- `sqlite` for stronger integrity under contention

## Which import should I use?

Start with this unless you already know you want something else:

```python
from pistributer import Pistributer
```

Use this quick rule:

- use `Pistributer` when you want the normal default experience
- use `PistributerTxt` when you want the raw plain-text path
- use `PistributerSqlite` when correctness under contention matters more than raw append speed

## When to use each driver

| Driver | Use it when | Avoid it when |
| --- | --- | --- |
| `txt` | You want the shortest plain-text file path and staged or single-writer-friendly queueing | You need structured records or stronger overlap correctness |
| `jsonl` | You want readable structured payloads and the default publishable workflow | You need the rawest append path or heavy overlapping write/read integrity |
| `sqlite` | You want stronger local correctness under contention | You want the lightest append-heavy file path |

## Project background

The earliest version of this tool was not created as a polished open-source package. It was created by a developer who wanted a queue but did not want to install or operate a heavier system such as Redis or Kafka.

The original approach was simple: use the filesystem itself as the message layer and use plain `.txt` files for data exchange. That early code was not cleaned up for publication, but the underlying queue logic was already useful in practice and was used on large amounts of real data.

Later, after the developer started building AI-oriented workflows with `nanobot`, the need for a cleaner installable package became more obvious. That is what pushed the repackaging effort: not a rewrite for the sake of novelty, but a practical need for `pip` installability, clearer docs, and a structured default format.

That is why the project now has two historical truths at the same time:

- the `0.1.x` line represents long-used file-queue logic that existed before the packaging cleanup
- the `0.2.0` line makes `.jsonl` the default public driver and adds wider tests, benchmarks, and release tooling

The current repository keeps that history visible instead of hiding it. The old behavior is preserved for comparison in `benchmarks/pistributer_bak.py`, while the main package is documented and tested as the public installable tool.

## `0.1.x` and `0.2.0` in practice

The simplest way to understand the version difference is this:

- `0.1.x` means the long-used plain-text file-queue era
- `0.2.0` means the public packaged era with `.jsonl` as the default driver

What changed:

- `0.1.x` was centered on the shortest `.txt` path and real-world usage before packaging cleanup
- `0.2.0` keeps the same basic file-queue idea but makes `.jsonl` the default public format
- `0.2.0` adds tests, benchmark documentation, packaging, and a clearer boundary for when to use `txt`, `jsonl`, and `sqlite`

What did **not** change:

- the project still values write/read throughput over interface growth
- the core file-queue idea is still append, rotate into `.in_use`, and consume with a small API
- the hot path is still expected to stay small and fast

Measured conclusion from the current benchmark work:

- the visible slowdown from `0.1.x` to `0.2.0` is real, but it is not mostly caused by the `.jsonl` extension itself
- most of the remaining overhead comes from the modern hot path doing a little more work around path validation and file-operation safeguards
- JSON serialization adds a smaller extra cost when callers pass Python objects instead of already-serialized strings

In short: `0.2.0` is slower than the historical plain-text path, but the gap is now mostly the cost of a cleaner public default and structured serialization, not a dramatic penalty from JSONL as a format.

## Design priorities

The author's preference is very explicit:

1. keep the queue fast under heavy write/read workloads
2. keep the public surface small
3. avoid changes that reduce throughput unless the trade-off is clearly worth it

That means this project does **not** try to win by offering a large interface. It tries to win by doing a very small number of things well:

- append records fast
- read records fast
- handle many files
- stay useful under high write pressure

The current `.jsonl` default is accepted even though it is slower than the historical `.txt` path, because the measured overhead is still within an acceptable range for the intended workflows. Outside of that specific trade-off, performance regressions are treated as unacceptable.

## Project position

`pistributer` is best positioned as a lightweight local queue for scripts, batch jobs, and single-host pipelines.

It is a good fit when:

- your workload is file-centric
- you want simple deployment with no external service
- you care about readable queue state on disk
- you want to choose between throughput-first and integrity-first local drivers
- your file-driver usage is staged or single-writer-friendly

It is not the best fit when:

- you need a multi-node distributed queue
- you need strict correctness under heavy overlapping readers and writers with the file drivers
- you want managed persistence, replication, or cross-host coordination

## Why use it

Use `pistributer` when you want a small queue abstraction for a local or single-host pipeline without introducing Redis, Kafka, or a separate database service.

What it is good at:

- append-heavy local workloads
- high-concurrency write-focused workloads
- simple batch pipelines and scripts
- readable on-disk data formats
- lightweight deployment with minimal operational overhead

The file-based drivers rotate active data into `.in_use`, which helps reduce direct producer/consumer contention on the same file.

The two file drivers, `Pistributer` and `PistributerTxt`, are best documented as staged or single-writer-friendly. They work well when appends and reads are mostly separated, but they are not the strongest option for heavy overlapping writer and reader contention.

For hot-path performance, `put()` in the two file drivers assumes the parent directory already exists. Directory preparation is intentionally kept outside the hot path.

The intent is not to keep adding more actions and more surface area. The intent is to keep the important path small: write, read, and move a lot of data without unnecessary slowdown.

## Install

For most users, this is the correct install command:

```bash
python3 -m pip install pistributer
```

If you want to upgrade later:

```bash
python3 -m pip install --upgrade pistributer
```

To confirm the package is installed:

```bash
python3 - <<'PY'
import pistributer
print(pistributer.__version__)
PY
```

If your system uses `python` instead of `python3`, you can use that instead.

## First 5 minutes

This section is intentionally written for beginners.

### Step 1: make a folder for queue files

```python
from pathlib import Path

queue_dir = Path("queues")
queue_dir.mkdir(parents=True, exist_ok=True)
```

This matters because the file drivers do **not** create parent folders automatically.

### Step 2: write messages into the queue

```python
from pistributer import Pistributer

Pistributer.put("queues/tasks.jsonl", {"task": "download", "id": 1})
Pistributer.put("queues/tasks.jsonl", {"task": "process", "id": 2})
```

### Step 3: read messages back out

```python
from pistributer import Pistributer

queue = Pistributer("queues/tasks.jsonl")

print(queue.next())
print(queue.next())
print(queue.isEmpty())
```

### Step 4: use one complete example

```python
from pathlib import Path

from pistributer import Pistributer

Path("queues").mkdir(parents=True, exist_ok=True)

Pistributer.put("queues/tasks.jsonl", {"task": "download", "id": 1})
Pistributer.put("queues/tasks.jsonl", {"task": "process", "id": 2})

queue = Pistributer("queues/tasks.jsonl")

while not queue.isEmpty():
    print(queue.next())
```

## Beginner-friendly usage patterns

### Use case: one script writes, another script reads

Writer:

```python
from pathlib import Path

from pistributer import Pistributer

Path("queues").mkdir(parents=True, exist_ok=True)

for index in range(5):
    Pistributer.put("queues/jobs.jsonl", {"job_id": index, "status": "queued"})
```

Reader:

```python
from pistributer import Pistributer

queue = Pistributer("queues/jobs.jsonl")

while not queue.isEmpty():
    item = queue.next()
    print(item)
```

### Use case: plain text instead of JSON

```python
from pathlib import Path

from pistributer_txt import PistributerTxt

Path("queues").mkdir(parents=True, exist_ok=True)

PistributerTxt.put("queues/logs.txt", "hello")
PistributerTxt.put("queues/logs.txt", "world")

queue = PistributerTxt("queues/logs.txt")

while not queue.isEmpty():
    print(queue.next())
```

### Use case: stronger local correctness with SQLite

```python
from pistributer_sqlite import PistributerSqlite

queue = PistributerSqlite("queues/tasks.db")
queue.put("hello")
queue.put("world")

print(queue.next())
print(queue.next())
print(queue.is_empty())

queue.close()
```

## Driver modes

### `jsonl` driver

```python
from pistributer import Pistributer
```

Use this as the default mode when you want structured records and a modern `.jsonl` workflow.

### `txt` driver

```python
from pistributer_txt import PistributerTxt
```

Use this when raw text throughput matters more than structure.

### `sqlite` driver

```python
from pistributer_sqlite import PistributerSqlite
```

Use this when queue correctness matters more than the shortest append path.

See `DRIVERS.md` for a full comparison.

See `EXAMPLES.md` for small copyable examples for all three drivers.

The Python docstrings are written to be `help()`-friendly, so `help(Pistributer)`, `help(Pistributer.put)`, and the equivalent driver methods should now give usable inline guidance.

## Choose a driver

| Driver | Best for | Main trade-off |
| --- | --- | --- |
| `Pistributer` (`jsonl`) | Structured local queues and readable payloads | More overhead than raw text |
| `PistributerTxt` | Fast plain-text append-heavy workloads | Least structured format |
| `PistributerSqlite` | Stronger local correctness under contention | Higher transaction overhead |

## API naming note

Public API names stay stable on purpose.

- `Pistributer` and `PistributerTxt` keep the historical camelCase names such as `isEmpty()` and `getIndex()`.
- `PistributerSqlite` keeps its newer snake_case method `is_empty()`.

The naming is not perfectly uniform, but the project keeps the existing public contract instead of breaking working code.

## Quick start

```python
from pistributer import Pistributer

Pistributer.put("channel.jsonl", {"value": "hello"})
Pistributer.put("channel.jsonl", {"value": "world"})

queue = Pistributer("channel.jsonl")

print(queue.next())
print(queue.next())
print(queue.isEmpty())
```

## Common beginner mistakes

### Mistake 1: writing to a folder that does not exist

This fails:

```python
from pistributer import Pistributer

Pistributer.put("missing/tasks.jsonl", {"task": "hello"})
```

Create the folder first:

```python
from pathlib import Path

Path("missing").mkdir(parents=True, exist_ok=True)
```

### Mistake 2: using the wrong file extension

- `Pistributer` expects `.jsonl`
- `PistributerTxt` expects `.txt`
- `PistributerSqlite` expects `.db`

Examples:

```python
from pistributer import Pistributer
from pistributer_txt import PistributerTxt
from pistributer_sqlite import PistributerSqlite

jsonl_queue = Pistributer("queues/tasks.jsonl")
txt_queue = PistributerTxt("queues/tasks.txt")
sqlite_queue = PistributerSqlite("queues/tasks.db")
```

### Mistake 3: assuming this is a distributed queue service

`pistributer` is best for local, file-based workflows. It is not trying to replace a multi-node broker.

### Mistake 4: choosing the wrong driver

If you are unsure, use the default `jsonl` driver first:

```python
from pistributer import Pistributer
```

Switch to `txt` only when you specifically want a raw plain-text file path.
Switch to `sqlite` when you specifically need stronger correctness under overlapping access.

## Help and discovery

You can inspect the built-in Python help:

```python
from pistributer import Pistributer

help(Pistributer)
help(Pistributer.put)
```

For more examples:

- see `EXAMPLES.md`
- see `DRIVERS.md`
- see `BENCHMARKS.md`

## Core API

The practical core of the project is intentionally small: append with `put()` and consume with `next()`.

The other methods exist to support queue lifecycle and compatibility, not to turn the library into a broad interface framework.

- `Pistributer(path)`: open a queue backed by a `.jsonl` file
- `Pistributer.put(path, value)`: append one message; the parent directory must already exist
- `Pistributer.new(path, value, overwrite=False, sep="")`: create a file with initial content
- `Pistributer.next()`: return the next message, or raise `StopIteration` if empty
- `Pistributer.isEmpty()`: return whether unread messages remain
- `Pistributer.size()`: count queued messages across active files
- `Pistributer.remaining()`: count unread messages
- `Pistributer.getIndex()`: return the consumed message count

## Validation

The repository includes both correctness tests and direct benchmarks against the preserved backup implementation at `benchmarks/pistributer_bak.py`.

### Functional tests

The main regression test writes `300` `.jsonl` files, each with `30` distinct JSON rows, and then consumes everything through `next()`.

That covers `9000` records and checks:

- FIFO ordering
- empty-queue behavior
- remaining-count behavior
- reopen/index persistence
- rotated `.in_use` files plus newly appended data

Run the test suite with:

```bash
python -m unittest discover -s tests -v
```

### Staged benchmark

The staged benchmark compares:

- `benchmarks/pistributer_bak.py` with `300` `.txt` files × `30` rows
- `pistributer.py` with `300` `.jsonl` files × `30` rows

Latest measured result in this workspace:

- backup total: about `0.956s`
- current total: about `1.020s`
- current write ratio: about `1.18x` of backup
- current read ratio: about `1.02x` of backup
- current total ratio: about `1.07x` of backup

In this staged workload, the current `jsonl` rewrite is slightly slower than the backup `txt` path, which is expected because the structured driver pays extra serialization and validation overhead.

That overhead is currently accepted because the repository intentionally chose `.jsonl` as the public default. Beyond that known trade-off, the project should resist changes that make the hot path slower.

The important version-difference detail is that this benchmark compares the historical backup implementation against the current public package. It is not a pure `jsonl` vs `txt` format test in isolation.

After moving parent-directory preparation out of the file-driver `put()` hot path, a focused write-only microbenchmark in this workspace produced these averages over five runs of `20000` appends:

- backup `txt` string append: about `0.617s`
- current `txt` string append: about `0.586s`
- current `jsonl` string append: about `0.608s`
- current `jsonl` dictionary append: about `0.652s`

That means the current `jsonl` hot path is now much closer to the historical reference point when the caller passes pre-serialized strings, and the remaining extra cost mainly shows up when the caller asks the driver to serialize Python objects.

Run it with:

```bash
python benchmarks/compare_versions.py
```

Detailed notes are in `BENCHMARKS.md`.

### Interleaved write/read stress benchmark

The repository also includes a heavier benchmark where writes and reads overlap:

- `300` writer threads
- `64` consumer threads
- `30` logical file assignments per writer
- `10` shared files per writer
- `300` rows per file assignment
- `2,700,000` total rows attempted
- `6010` physical files after shared-path collapse

Latest measured result in this workspace:

- backup `.txt` consumed `2,699,852 / 2,700,000`
- current `.jsonl` consumed `2,699,351 / 2,700,000`
- both modes produced `0` malformed JSON rows
- both file drivers failed full integrity under simultaneous write/read pressure

That is the practical reason the `sqlite` driver exists: once the workload shifts from staged local queueing to stronger overlapping write/read correctness, a transactional driver becomes the better fit.

Run it with:

```bash
python benchmarks/threaded_interleaved_compare.py
```

## Release

Release steps and commands are documented in `RELEASE.md`.

Project history is tracked in `CHANGELOG.md`.

## Contributing

Small, focused contributions are welcome. Start with `CONTRIBUTING.md` for development and review expectations.

## Notes

- Messages are stored as one JSON object per line in the default driver.
- Files rotate from `data` to `.in_use` so new appends stay separate from active consumption.
- `txt` and `jsonl` are strongest in local staged workloads where writes and reads are mostly separated.
- `sqlite` targets a different optimization goal: stronger correctness under concurrency.
- The project is intentionally lightweight and local-first.

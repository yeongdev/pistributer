# Driver modes

`pistributer` documents three practical drivers because no single local queue design optimizes speed, structure, and concurrency integrity at the same time.

The project is intentionally performance-first. It would rather keep a smaller interface and a faster hot path than add more API surface that slows down write or read throughput.

## `txt` driver

Module:

- `pistributer_txt.py`

Import:

```python
from pistributer_txt import PistributerTxt
```

Best for:

- raw local throughput
- already-serialized string records
- workflows that want behavior closest to the original file driver
- staged or single-writer-friendly file queue workloads

Trade-offs:

- least structured payload format
- not the strongest option for overlapping write/read correctness
- `put()` assumes the parent directory already exists

## `sqlite` driver

Module:

- `pistributer_sqlite.py`

Import:

```python
from pistributer_sqlite import PistributerSqlite
```

Best for:

- stronger correctness under concurrent access
- deterministic local queue state
- workloads where integrity matters more than the shortest IO path

Trade-offs:

- slower than the raw file path in many append-heavy cases
- higher storage and transaction overhead than plain file append

## `jsonl` driver

Module:

- `pistributer.py`

Import:

```python
from pistributer import Pistributer
```

Best for:

- structured records that stay easy to inspect and replay
- modern `.jsonl` pipelines
- the default publishable experience of the project
- staged or single-writer-friendly file queue workloads

Trade-offs:

- slightly more overhead than the pure `txt` path
- still a file driver, so not the strongest option for extreme overlapping write/read integrity
- `put()` assumes the parent directory already exists

## Why all three exist

The split is practical, not theoretical:

- `txt`: shortest path, throughput first
- `sqlite`: integrity first
- `jsonl`: structured default

That split also reflects the performance rule of the project:

- `txt` remains the raw reference point for the shortest file path
- `jsonl` is accepted as the public default even with measurable overhead
- `sqlite` exists for a different goal, not because the project wants a larger API

API naming also stays practical:

- the file drivers keep the historical camelCase methods such as `isEmpty()`
- the SQLite driver keeps its existing `is_empty()` method
- the project keeps those names stable instead of breaking compatibility

The benchmarks in `BENCHMARKS.md` show that the file-based modes are competitive in staged local workloads, while the interleaved stress test explains why the integrity-oriented SQLite mode is still important.

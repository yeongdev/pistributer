# Benchmarks

This document explains the benchmark set used to compare the current `pistributer.py` implementation with the preserved backup implementation in `benchmarks/pistributer_bak.py`.

The preserved backup benchmark no longer requires `requests` just to import and run local comparisons.

The purpose is not to claim that file queues outperform every external queue. The purpose is to verify that the current implementation remains faithful to the original local high-throughput design, while also documenting where each driver mode fits best.

The benchmark philosophy is also simple: if a change slows the hot path, it needs a very strong reason. The main accepted exception in the current public package is the move from the historical `.txt` path to the structured `.jsonl` default.

## `0.1.x` vs `0.2.0`

For practical reading, the benchmark comparison is a comparison between two eras of the project:

- `0.1.x`: the historical plain-text file-queue path represented by `benchmarks/pistributer_bak.py`
- `0.2.0`: the current public package with `.jsonl` as the default driver

This matters because the staged benchmark is not measuring only file suffix differences. It is measuring the historical implementation against the current implementation.

## Benchmark goals

The benchmarks answer two practical questions:

- Does the modern `jsonl` rewrite stay competitive with the original file queue?
- Where do the file-based drivers stop being the right tool, making `sqlite` the safer choice?

## Benchmark 1: staged write then consume

The staged benchmark creates:

- `300` channel files
- `30` rows per file
- `9000` total records

It then measures two phases for each implementation:

1. append all rows
2. consume all rows with `next()`

Compared implementations:

- `benchmarks/pistributer_bak.py` using `.txt`
- `pistributer.py` using `.jsonl`

### Staged command

```bash
python benchmarks/compare_versions.py
```

### Staged result

One measured run in this workspace:

- backup write: `0.283s`
- backup read: `0.673s`
- backup total: `0.956s`
- current write: `0.334s`
- current read: `0.686s`
- current total: `1.020s`

Relative to the backup:

- write ratio: `1.18x`
- read ratio: `1.02x`
- total ratio: `1.07x`

These staged totals are still useful as an end-to-end snapshot, but they are not the best way to judge small write-path changes because filesystem timing variance can dominate the difference. For hot-path append analysis, the focused write-only microbenchmark below is the better signal.

### Staged interpretation

The current implementation is slightly slower on writes, slightly slower on reads, and slightly slower overall in this staged workload.

That still matters because `pistributer` is not only about append speed. It is about preserving the local queue lifecycle while offering a structured default format and a small API.

### What actually slows `0.2.0`

Focused local profiling and microbenchmarks show that the slowdown is not mainly caused by the word `jsonl` or by JSON parsing on the read path.

The most likely contributors are:

1. file I/O fixed costs on every append, especially `open`, `close`, and `flush`
2. extra hot-path safeguards in the current public implementation, such as path validation
3. JSON serialization when the caller passes Python objects instead of already-serialized strings

Note: parent-directory creation used to be part of the current hot path, but it has now been removed from `put()` in the file drivers. The parent directory must already exist before hot-path writes.

The measured local interpretation is:

- current `jsonl` vs current `txt` with string payloads is only modestly slower
- passing dictionaries to the `jsonl` driver adds another smaller serialization cost
- the larger visible gap in the staged benchmark includes the fact that the historical backup path is a different implementation, not just a different extension

### Focused write-only microbenchmark

After removing parent-directory creation from the file-driver `put()` hot path, a focused write-only microbenchmark in this workspace averaged:

- backup `txt` string append: `0.617s`
- current `txt` string append: `0.586s`
- current `jsonl` string append: `0.608s`
- current `jsonl` dictionary append: `0.652s`

Useful ratios from that benchmark:

- current `txt` vs backup `txt`: `0.951x`
- current `jsonl` string vs backup `txt`: `0.986x`
- current `jsonl` string vs current `txt`: `1.037x`
- current `jsonl` dictionary vs current `jsonl` string: `1.073x`

Interpretation:

- once directory preparation is removed from the hot path, the current `jsonl` string append path is close to the historical plain-text reference
- the remaining structural gap is small for already-serialized strings
- the clearest remaining JSONL-specific cost appears when the driver serializes Python objects on behalf of the caller

So the practical version conclusion is:

- `0.1.x` remains the raw plain-text reference point for the shortest hot path
- `0.2.0` accepts a moderate overhead in exchange for a structured public default, packaging, and clearer project boundaries

## Benchmark 2: simultaneous write and read

The second benchmark is intentionally harsher: it overlaps producers and consumers.

Scenario:

- `300` writer threads
- `64` consumer threads
- `30` logical file assignments per writer
- `10` shared files per writer
- `300` rows per file assignment
- `2,700,000` total rows attempted
- `6010` physical files after shared targets collapse onto the same paths

### Interleaved command

```bash
python benchmarks/threaded_interleaved_compare.py
```

### Interleaved result

Measured in this workspace:

- backup write phase: `213.949s`
- backup total: `367.203s`
- backup consumed: `2,699,852 / 2,700,000`
- current write phase: `233.246s`
- current total: `386.073s`
- current consumed: `2,699,351 / 2,700,000`

Integrity outcome:

- backup malformed rows: `0`
- current malformed rows: `0`
- backup full integrity: `false`
- current full integrity: `false`

### Interleaved interpretation

This benchmark defines the boundary of the file-based drivers.

Under simultaneous write/read pressure, both file drivers lose rows in this stress setup. That does not make them useless. It means they are best suited to staged local workloads rather than the strongest overlapping producer/consumer correctness requirements.

That is why the project now documents three modes clearly:

- `txt`: raw-speed file path
- `jsonl`: structured modern file path
- `sqlite`: integrity-oriented local queue

Why the current `jsonl` path is slower than the original `txt` path in the heavier benchmark:

- the original `txt` driver is close to bare file append plus rotation
- the `jsonl` driver adds structure and validation on the hot path
- the extra Python overhead is small per operation, but visible when repeated millions of times

## Caveat

These numbers are local benchmark results, not universal guarantees. Filesystem type, disk speed, Python version, concurrency pattern, and record size all matter.

If your workload is important, rerun the benchmarks with your own payload sizes and concurrency profile.

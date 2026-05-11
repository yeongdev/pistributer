# Changelog

All notable changes to `pistributer` should be documented in this file.

The format is inspired by Keep a Changelog, and the project follows semantic versioning where practical.

## [Unreleased]

### Unreleased - Added

- Added prompt-friendly public API docstrings across the three supported drivers.
- Added a built-in help section in `EXAMPLES.md` to make Python `help()` a documented discovery path.
- Added a failure-boundary example in `EXAMPLES.md` to show when the file drivers are not the right tool.

### Unreleased - Changed

- Documented the file drivers as staged or single-writer-friendly in code and docs.
- Documented the stable mixed API naming contract instead of changing public method names.
- Strengthened the README entrypoint with a sharper positioning statement, a driver selection table, and a project background section.
- Documented the performance-first project contract, including the preference for minimal API growth and resistance to throughput regressions beyond the accepted `.jsonl` overhead.
- Documented the practical `0.1.x` vs `0.2.0` difference and clarified that the measured slowdown is mostly about file I/O fixed costs and structured serialization overhead, with earlier hot-path checks reduced further over time.
- Removed parent-directory creation from the file-driver `put()` hot path and documented that parent directories must already exist before hot-path writes.
- Updated the benchmark docs with focused write-only microbenchmark results after the hot-path directory-preparation removal.

### Unreleased - Fixed

- Removed the historical benchmark import burden on `requests` and kept the comparison scripts runnable with the standard library only.

## [0.2.0] - 2026-05-11

### 0.2.0 - Added

- Packaged the project for installation with `pyproject.toml`.
- Added practical driver separation across `jsonl`, `txt`, and `sqlite` modes.
- Added unit tests covering queue behavior and driver round-trips.
- Added benchmark scripts for staged and interleaved workload comparisons.
- Added GitHub-ready repository metadata, CI workflow, issue templates, PR template, and contribution guide.
- Added `DRIVERS.md`, `BENCHMARKS.md`, and `RELEASE.md` to document behavior, trade-offs, and release steps.

### 0.2.0 - Changed

- Standardized the main driver on `.jsonl` for the default publishable workflow.
- Preserved the legacy implementation as `benchmarks/pistributer_bak.py` for comparison and historical reference.
- Standardized repository documentation to English for public release.

### 0.2.0 - Fixed

- Removed packaging and documentation inconsistencies that blocked a clean public release flow.
- Fixed package metadata so `python -m build` and `python -m twine check dist/*` succeed.

# Contributing

Thanks for contributing to `pistributer`.

This project aims to stay small, local-first, and explicit about the trade-offs between `txt`, `jsonl`, and `sqlite` drivers. Please keep changes focused and aligned with that scope.

## Development setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
python -m unittest discover -s tests -v
```

## What makes a good contribution

Good changes usually have one of these properties:

- improve correctness without changing the project scope
- improve documentation, benchmarks, or release clarity
- improve one driver without weakening the others
- make trade-offs more explicit for users

## Before opening a pull request

Please try to:

- keep the change scoped to one problem
- update documentation if behavior or positioning changes
- run the relevant tests locally
- include benchmark notes if performance changes

## Benchmark-sensitive changes

If your change affects queue throughput, file rotation, or concurrency behavior, include at least one of these commands in your validation notes:

```bash
python benchmarks/compare_versions.py
python benchmarks/threaded_interleaved_compare.py
```

## Scope guidance

`pistributer` is intentionally not trying to become a distributed broker or a general database abstraction layer.

Changes are more likely to fit if they help one of these goals:

- local queue ergonomics
- readable on-disk queue data
- practical driver selection
- clear benchmark and release workflows

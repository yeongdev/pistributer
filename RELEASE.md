# Release checklist

## 1. Update the version

Update the version in:

- `pyproject.toml`
- `pistributer.py`

## 2. Run validation

Run the test suite:

```bash
python -m unittest discover -s tests -v
```

Optional staged benchmark against the backup implementation:

```bash
python benchmarks/compare_versions.py
```

Optional interleaved write/read stress benchmark:

```bash
python benchmarks/threaded_interleaved_compare.py
```

Optional editable-install smoke test:

```bash
python -m pip install -e .
```

## 3. Build distributions

Install tooling if needed:

```bash
python -m pip install --upgrade build twine
```

Build source and wheel distributions:

```bash
python -m build
```

Artifacts are written to `dist/`.

## 4. Validate package metadata

```bash
python -m twine check dist/*
```

## 5. Upload to TestPyPI

```bash
python -m twine upload --repository testpypi dist/*
```

Verify installation from TestPyPI:

```bash
python -m pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple pistributer
```

## 6. Upload to PyPI

```bash
python -m twine upload dist/*
```

## 7. Tag the release

```bash
git tag v0.2.0
git push origin v0.2.0
```

## Authentication note

`twine` can prompt for credentials, or you can configure `~/.pypirc`.

Common token-based setup:

- username: `__token__`
- password: your PyPI token

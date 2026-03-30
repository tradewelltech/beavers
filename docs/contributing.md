# Contributing

Welcome! We're happy to have you here. Thank you in advance for your contribution to Beavers.

## Development environment set up

```shell
uv sync --group dev
uvx prek install
```

## Testing

To run tests fast:

```shell
uv run pytest -n auto tests
```

To Get coverage:

```shell
uv run coverage run --branch --rcfile=./pyproject.toml --include "./beavers/*" -m pytest tests
uv run coverage report --show-missing
```

## New Release

Tag and push:

```shell
git tag vX.X.X
git push origin vX.X.X
```

Lastly on github, go to tags and create a release.
The CI will deploy to pypi automatically from then.

## Testing the documentation

```shell
uv run --group docs mkdocs serve --livereload --watch=./
```

## Updating dependencies

- For the repo `uv lock --upgrade`
- For the doc: `uv pip compile docs/requirements.in -o docs/requirements.txt`
- For pre-commit: `uvx prek autoupdate`

## Resources

The repo set up is inspired by this [guide](https://mathspp.com/blog/how-to-create-a-python-package-in-2022)

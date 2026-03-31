# Contributing

Welcome! We're happy to have you here. Thank you in advance for your contribution to Beavers.

Most development tasks are defined in the [justfile](https://github.com/tradewelltech/beavers/blob/main/justfile).

## Development environment set up

```shell
uv sync --all-groups --all-extras
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

Create a new release, from a new tag in github. Autofill the change log.

## Testing the documentation

```shell
uv run --group docs mkdocs serve --livereload --watch=./
```

## Updating dependencies

- For the repo `uv lock --upgrade`
- For the doc: `uv pip compile docs/requirements.in -o docs/requirements.txt`
- For prek: `uvx prek autoupdate`

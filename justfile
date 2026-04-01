# Set up the development environment
setup:
    uv sync --all-groups --all-extras
    uvx prek install

# Run tests
test *args:
    uv run pytest tests {{ args }}

# Run tests with coverage and report
coverage:
    uv run coverage run --branch --rcfile=pyproject.toml --source=beavers -m pytest tests
    uv run coverage report --rcfile=pyproject.toml -m --fail-under 95

# Run examples
examples:
    uv run python -c 'import examples.advanced_concepts'
    uv run python -c 'import examples.dag_concepts'
    uv run python -c 'import examples.etfs'
    uv run python -c 'import examples.polars_concepts'
    uv run python -c 'import examples.pyarrow_concepts'
    uv run python -c 'import examples.replay_concepts'
    uv run python -c 'import examples.perspective_concepts'

# Run linting (pre-commit on all files)
lint:
    uvx prek run --all-files

# Build the documentation
docs-build:
    uv run --group docs mkdocs build

# Serve the documentation with live reload
docs-serve:
    uv run --group docs mkdocs serve --livereload --watch=./

# Update all dependencies
update:
    uv lock --upgrade
    uv pip compile docs/requirements.in -o docs/requirements.txt
    uvx prek autoupdate

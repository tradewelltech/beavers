# Contributing

Welcome! We're happy to have you here. Thank you in advance for your contribution to Beavers.

## Development environment set up

```shell
python3 -m venv --clear venv
source venv/bin/activate
poetry self add "poetry-dynamic-versioning[plugin]"
poetry install
pre-commit install
```

## Testing

To run tests fast:

```shell
pytest -n auto tests
```

To Get coverage:

```shell
coverage run --branch --rcfile=./pyproject.toml --include "./beavers/*" -m pytest tests
coverage report --show-missing
```

## Generating the change log

We use [git-change-log](https://pawamoy.github.io/git-changelog/usage/) to generate our CHANGELOG.md

Please follow the [basic convention](https://pawamoy.github.io/git-changelog/usage/#basic-convention) for commit
message.

To update the change log, run:

```shell
git-changelog -io CHANGELOG.md
```

## New Release

For new release, first prepare the change log, push and merge it.

```shell
git-changelog --bump=auto -io CHANGELOG.md
```

Then tag and push:

```shell
git tag vX.X.X
git push origin vX.X.X
```

Lastly on github, go to tags and create a release.
The CI will deploy to pypi automatically from then.

## Testing the documentation

```shell
mkdocs serve --livereload --watch=./
```

## Updating dependencies

- For the repo `poetry update`
- For the doc: `(cd docs/; pip-compile ./requirements.in  > ./requirements.txt)`
- For pre-commit: `pre-commit autoupdate`

## Resources

The repo set up is inspired by this [guide](https://mathspp.com/blog/how-to-create-a-python-package-in-2022)

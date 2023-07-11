from pathlib import Path


def test_readme_and_index_same():
    """Check the README matches the doc home page"""
    root = Path(__file__).parent.parent
    readme = root / "README.md"
    index = root / "docs" / "index.md"

    with readme.open() as fp:
        readme_content = fp.read()

    with index.open() as fp:
        index_content = fp.read()

    assert index_content in readme_content

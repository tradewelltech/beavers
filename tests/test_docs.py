from pathlib import Path


def test_readme_and_index_same():
    """Check the README matches the doc home page"""
    root = Path(__file__).parent.parent
    readme = root / "README.md"
    index = root / "docs" / "index.md"

    with readme.open() as fp:
        readme_content = fp.read()

    with index.open() as fp:
        # Skip first and last line
        index_content = "".join(fp.readlines()[1:-1])

    assert index_content in readme_content

"""Fail when README.md's configuration reference misses an option that the package accepts.

The options come from the package source: the keys that parquet_direct__known_keys accepts, and the arguments of
the generic test and the run-operation. Each option with its own row must be the first cell of a table row under its
"###" heading in the "## Configuration reference" section, so an option documented under another heading does not
count; the rest must appear in the section in backticks.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def known_keys() -> dict[str, list[str]]:
    source = (ROOT / "macros/parquet_direct/validate.sql").read_text()
    start = source.index("{% macro parquet_direct__known_keys()")
    block = source[start : source.index("{%- endmacro %}", start)]
    groups = {}
    for group, body in re.findall(r"'([a-z_]+)':\s*\[([^\]]*)\]", block):
        groups[group] = re.findall(r"'([a-z_]+)'", body)
    return groups


def macro_arguments(path: str, signature: str) -> list[str]:
    source = (ROOT / path).read_text()
    match = re.search(re.escape(signature) + r"\(([^)]*)\)", source)
    if not match:
        raise SystemExit(f"{path}: cannot find {signature}(...)")
    return [argument.split("=")[0].strip() for argument in match.group(1).split(",") if argument.strip()]


def main() -> int:
    readme = (ROOT / "README.md").read_text()
    if "## Configuration reference" not in readme:
        print("README.md has no '## Configuration reference' section")
        return 1
    section = readme.split("## Configuration reference", 1)[1].split("\n## ", 1)[0]
    rows_by_heading = {
        heading: set(re.findall(r"^\| `([a-z_.]+)` \|", body, re.MULTILINE))
        for heading, body in (part.split("\n", 1) for part in section.split("\n### ")[1:])
    }

    def rows_under(prefix: str) -> set[str]:
        headings = [heading for heading in rows_by_heading if heading.startswith(prefix)]
        if len(headings) != 1:
            raise SystemExit(f"README.md configuration reference: expected one '### {prefix}...' heading, found {len(headings)}")
        return rows_by_heading[headings[0]]

    keys = known_keys()
    test_arguments = macro_arguments("macros/tests/parquet_direct_no_skipped_files.sql", "{% test parquet_direct_no_skipped_files")
    operation_arguments = macro_arguments("macros/operations/parquet_direct_find_skipped_files.sql", "{% macro parquet_direct_find_skipped_files")
    with_rows = {
        "Table options": keys["top"] + keys["row_access_policy"],
        "Columns": keys["column"],
        "Registration check": keys["registration_check"],
        "Constraints": keys["constraint"],
        "Previous versions": keys["previous_version"],
        "Generic test": [name for name in test_arguments if name != "model"],
        "Run-operation": operation_arguments,
    }
    in_text = {group: keys[group] for group in ("inline_constraint", "references", "masking_policy")}

    missing = [f"`{name}` has no table row under '### {heading}'" for heading, names in with_rows.items() for name in names if name not in rows_under(heading)]
    missing += [f"{group} `{name}` is not mentioned" for group, names in in_text.items() for name in names if f"`{name}`" not in section]
    for line in missing:
        print(f"README.md configuration reference: {line}")
    if not missing:
        print(f"README.md configuration reference lists all {sum(map(len, with_rows.values())) + sum(map(len, in_text.values()))} options")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())

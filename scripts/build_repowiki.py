#!/usr/bin/env python3
"""Build the bilingual MkDocs source tree from the transport README."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import NoReturn


REPO_ROOT = Path(__file__).resolve().parents[1]
WIKI_ROOT = REPO_ROOT / "repowiki"
DEFAULT_OUTPUT = REPO_ROOT / ".repowiki-site"
SOURCE = "README.md"
TARGET = "source-docs/overview.md"
SOURCE_URL = "https://github.com/NeKiro-project/nekiro-a2a-transport-go/blob/main/README.md"


def fail(message: str) -> NoReturn:
    raise ValueError(message)


def copy_tracked_wiki(output: Path) -> None:
    for path in WIKI_ROOT.rglob("*"):
        if path.is_dir():
            continue
        relative = path.relative_to(WIKI_ROOT)
        if relative.parts[0] == "zh":
            continue
        if relative.parts[0] == "assets":
            destination = output / relative
        elif path.suffix == ".md":
            destination = output / "en" / relative
        else:
            fail(f"unsupported tracked RepoWiki file: {relative}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
    for path in (WIKI_ROOT / "zh").rglob("*"):
        if path.is_dir():
            continue
        relative = path.relative_to(WIKI_ROOT)
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)


def validate() -> None:
    for relative in ("index.md", "zh/index.md", "assets/stylesheets/extra.css"):
        if not (WIKI_ROOT / relative).is_file():
            fail(f"missing RepoWiki input: repowiki/{relative}")
    if not (REPO_ROOT / SOURCE).is_file():
        fail("missing source document: README.md")
    for wiki_path in WIKI_ROOT.rglob("*.md"):
        text = wiki_path.read_text(encoding="utf-8")
        if "{{" in text or "relative_url" in text:
            fail(f"Jekyll/Liquid link remains in RepoWiki source: {wiki_path.relative_to(REPO_ROOT)}")


def source_page(language: str) -> str:
    if language == "zh":
        note = (
            '<div class="source-note">Canonical source：'
            f'<a href="{SOURCE_URL}"><code>{SOURCE}</code></a>。'
            "本页保留英文 canonical 正文，中文导航和入口已提供。</div>"
        )
    else:
        note = (
            '<div class="source-note">Canonical source: '
            f'<a href="{SOURCE_URL}"><code>{SOURCE}</code></a>. '
            "This page is rendered from the repository README during the MkDocs build.</div>"
        )
    return f"{note}\n\n{(REPO_ROOT / SOURCE).read_text(encoding='utf-8')}"


def build(output: Path) -> None:
    if output.exists():
        if output.is_dir():
            shutil.rmtree(output)
        else:
            output.unlink()
    output.mkdir(parents=True, exist_ok=True)
    copy_tracked_wiki(output)
    for language in ("en", "zh"):
        generated_root = output / language / "source-docs"
        generated_root.mkdir(parents=True, exist_ok=True)
        if language == "zh":
            index = "# 源文档\n\n以下页面从 transport 仓库的 canonical README 生成。\n"
        else:
            index = "# Source documentation\n\nThis page is generated from the canonical transport README.\n"
        (generated_root / "index.md").write_text(index, encoding="utf-8")
        (output / language / TARGET).write_text(source_page(language), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        validate()
        if args.check:
            print("RepoWiki check passed: 1 source document, 2 locales")
        else:
            output = args.output if args.output.is_absolute() else REPO_ROOT / args.output
            build(output)
            print(f"MkDocs source generated: {output}")
    except ValueError as error:
        print(f"RepoWiki build failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

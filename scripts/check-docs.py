#!/usr/bin/env python3
"""Check repository Markdown fences, local links and heading fragments offline.

Supports the inline links and ATX headings used in this repository. External
URLs and prose/code path references are outside this check.
"""

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]


def markdown(path):
    lines = []
    fence = None
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        marker = re.match(r"^\s{0,3}(`{3,}|~{3,})(.*)$", line)
        if marker:
            run, tail = marker.groups()
            if fence is None:
                fence = (run[0], len(run), number)
            elif run[0] == fence[0] and len(run) >= fence[1] and not tail.strip():
                fence = None
            continue
        if fence is None:
            lines.append((number, line))
    return lines, fence


def anchors(lines):
    found = set()
    counts = {}
    for _, line in lines:
        heading = re.match(r"^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        if not heading:
            continue
        value = heading[1].strip().lower()
        value = "".join(c for c in value if c.isalnum() or c in " _-")
        value = value.replace(" ", "-")
        count = counts.get(value, 0)
        counts[value] = count + 1
        found.add(value if count == 0 else f"{value}-{count}")
    return found


def main():
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
    ).decode().split("\0")
    paths = sorted({ROOT / name for name in names if name.endswith(".md")})
    documents = {}
    errors = []
    links = 0
    for path in paths:
        if not path.is_file():
            errors.append(f"{path.relative_to(ROOT)}: missing Markdown file")
            continue
        lines, fence = markdown(path)
        documents[path.resolve()] = (lines, anchors(lines))
        if fence:
            errors.append(f"{path.relative_to(ROOT)}:{fence[2]}: unclosed code fence")
    for path, (lines, _) in documents.items():
        for number, line in lines:
            line = re.sub(r"(`+).*?\1", "", line)
            for match in re.finditer(r"!?\[[^\]]*\]\((<[^>]+>|[^\s)]+)(?:\s+\"[^\"]*\")?\)", line):
                target = match[1].strip("<>")
                parsed = urlsplit(target)
                if parsed.scheme or parsed.netloc:
                    continue
                links += 1
                destination = (path.parent / unquote(parsed.path)).resolve() if parsed.path else path
                reason = None
                if destination != ROOT and ROOT not in destination.parents:
                    reason = "link escapes repository"
                elif not destination.exists():
                    reason = "missing local target"
                elif parsed.fragment and destination.suffix == ".md":
                    if destination not in documents:
                        reason = "Markdown target is excluded from repository inventory"
                    elif unquote(parsed.fragment) not in documents[destination][1]:
                        reason = "missing heading fragment"
                if reason:
                    errors.append(f"{path.relative_to(ROOT)}:{number}: {reason}: {target}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Checked {len(documents)} Markdown files and {links} local links; fences and fragments pass.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

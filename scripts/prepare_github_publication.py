"""Make a sanitized public Git history without rewriting the source repository.

Redaction values belong in an ignored local JSON file, never this script or Git.
The destination must be a new directory under artifacts. Only the sanitized
repository is suitable for publication after its separate history scan passes.
"""

import argparse
import io
import json
import subprocess
from pathlib import Path

EXCLUDED_PATH = b"deploy/vercel/project.json"


def redact(payload: bytes, replacements: dict[str, str]) -> bytes:
    for old in sorted(replacements, key=len, reverse=True):
        payload = payload.replace(old.encode(), replacements[old].encode())
    return payload


def sanitize_export(payload: bytes, replacements: dict[str, str], login: str) -> bytes:
    source, target = io.BytesIO(payload), io.BytesIO()
    identity = f"{login} <{login}@users.noreply.github.com>".encode()
    while line := source.readline():
        if line.startswith(b"original-oid "):
            continue
        if line.startswith(b"data "):
            length = int(line.split()[1])
            content = source.read(length)
            if len(content) != length:
                raise ValueError("truncated Git export")
            content = redact(content, replacements)
            target.write(f"data {len(content)}\n".encode())
            target.write(content)
            continue
        if line.startswith((b"author ", b"committer ")):
            role = line.split(b" ", 1)[0]
            timestamp = line.rsplit(b">", 1)[1]
            target.write(role + b" " + identity + timestamp)
            continue
        if line.startswith((b"M ", b"D ", b"R ", b"C ")) and EXCLUDED_PATH in line:
            continue
        target.write(redact(line, replacements))
    return target.getvalue()


def prepare(workspace: Path, destination: Path, redactions: dict[str, str], login: str):
    workspace, destination = workspace.resolve(), destination.resolve()
    if not destination.is_relative_to(workspace / "artifacts") or destination.exists():
        raise ValueError("publication must use a new directory under workspace artifacts")
    if not login or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in login.lower()):
        raise ValueError("invalid public GitHub login")
    destination.mkdir(parents=True)
    original = subprocess.check_output(
        ["git", "fast-export", "--show-original-ids", "refs/heads/main"], cwd=workspace
    )
    sanitized = sanitize_export(original, redactions, login)
    subprocess.run(["git", "init", "--initial-branch=main", str(destination)], check=True)
    subprocess.run(["git", "fast-import", "--quiet"], input=sanitized, cwd=destination, check=True)
    subprocess.run(["git", "reset", "--hard", "main"], cwd=destination, check=True)
    return destination


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--redactions", type=Path, required=True)
    parser.add_argument("--login", required=True)
    arguments = parser.parse_args()
    values = json.loads(arguments.redactions.read_text(encoding="utf-8"))
    result = prepare(Path.cwd(), arguments.destination, values, arguments.login)
    print(json.dumps({"publication_repository": str(result)}))

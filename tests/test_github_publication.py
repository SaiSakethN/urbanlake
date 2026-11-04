from scripts.prepare_github_publication import redact, sanitize_export


def test_public_export_preserves_lengths_and_drops_private_config():
    contents = b"private-value \xc3\xa9\n"
    message = b"private-value\n"
    export = (
        b"blob\nmark :1\noriginal-oid original\ndata "
        + str(len(contents)).encode()
        + b"\n"
        + contents
        + b"\ncommit refs/heads/main\nmark :2\n"
        + b"author Private Name <private@example.invalid> 1 +0000\n"
        + b"committer Private Name <private@example.invalid> 1 +0000\n"
        + b"data "
        + str(len(message)).encode()
        + b"\n"
        + message
        + b"M 100644 :1 public.txt\nM 100644 :1 deploy/vercel/project.json\n\n"
    )
    result = sanitize_export(export, {"private-value": "public"}, "example")
    assert b"private-value" not in result
    assert b"private@example.invalid" not in result
    assert b"deploy/vercel/project.json" not in result
    assert b"example@users.noreply.github.com" in result
    assert b"data 10\npublic \xc3\xa9\n" in result
    assert b"data 7\npublic\n" in result
    assert b"M 100644 :1 public.txt" in result


def test_longer_redactions_are_applied_first():
    assert redact(b"private-long private", {"private": "B", "private-long": "A"}) == b"A B"

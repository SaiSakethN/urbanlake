"""Durable atomic JSON publication with bounded Windows sharing-violation retries.

Readers can briefly hold the destination without delete-sharing on Windows,
including through a Docker bind mount. The completed temporary file stays in the
same directory and the old destination remains intact until replacement succeeds.
"""

import errno
import json
import os
from os import replace
from pathlib import Path
from time import sleep
from uuid import uuid4


def atomic_json(
    path: Path,
    value: dict,
    *,
    attempts: int = 12,
    retry_delay: float = 0.025,
    max_retry_delay: float = 0.5,
) -> None:
    if not 1 <= attempts <= 20 or retry_delay < 0 or max_retry_delay < 0:
        raise ValueError("attempts must be 1..20 and retry delays nonnegative")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(attempts):
            try:
                replace(temporary, path)
                return
            except OSError as error:
                sharing_error = (
                    isinstance(error, PermissionError)
                    or error.errno in (errno.EACCES, errno.EPERM, errno.EBUSY)
                    or getattr(error, "winerror", None) in (5, 32, 33)
                )
                if not sharing_error or attempt == attempts - 1:
                    raise
                sleep(min(retry_delay * 2**attempt, max_retry_delay))
    finally:
        # Only our uniquely named unpublished temporary file can be removed.
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            # A failed cleanup must not replace the original publication error.
            pass

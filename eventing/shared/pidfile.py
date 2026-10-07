"""PID file helper for the demo daemons.

`PidFile(name)` writes `${TMPDIR}/rossoctl-keda1/<name>.pid` on __enter__, removes
it on __exit__. Refuses to start if the file exists and points at a live process
(unless RUN_FORCE=1 is set in the env). Stale PID files (process gone, or naming
our own PID) are silently reclaimed.
"""
from __future__ import annotations

import errno
import os
import pathlib


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as e:
        return e.errno == errno.EPERM   # exists but we don't own it
    return True


def default_dir() -> pathlib.Path:
    base = os.environ.get("TMPDIR", "/tmp").rstrip("/")
    d = pathlib.Path(base) / "rossoctl-keda1"
    d.mkdir(parents=True, exist_ok=True)
    return d


class PidFile:
    def __init__(self, name: str, directory: pathlib.Path | None = None) -> None:
        self._path = (directory or default_dir()) / f"{name}.pid"
        self._name = name

    @property
    def path(self) -> pathlib.Path:
        return self._path

    def __enter__(self) -> "PidFile":
        if self._path.exists():
            try:
                existing = int(self._path.read_text().strip() or "0")
            except ValueError:
                existing = 0
            if existing == os.getpid():
                # We have not written this file yet, so our own PID in it means the
                # previous process died abruptly and the restarted one was handed the
                # same PID. In a container that is every restart: the process is PID 1,
                # `_pid_alive(1)` is True because PID 1 is the caller itself, and the
                # guard below would refuse to start forever ("already running as pid 1").
                print(f"[{self._name}] reclaimed stale pidfile naming our own pid"
                      f" {existing}: {self._path}")
            elif existing and _pid_alive(existing):
                if os.environ.get("RUN_FORCE") != "1":
                    raise SystemExit(
                        f"[{self._name}] already running as pid {existing}"
                        f" (pidfile: {self._path}). Stop it first with"
                        f" `kill -INT {existing}` or set RUN_FORCE=1 to override."
                    )
                print(f"[{self._name}] WARNING: overwriting live pidfile for pid {existing} (RUN_FORCE=1)")
            else:
                print(f"[{self._name}] reclaimed stale pidfile: {self._path}")
        self._path.write_text(f"{os.getpid()}\n")
        print(f"[{self._name}] pidfile: {self._path}")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self._path.exists() and self._path.read_text().strip() == str(os.getpid()):
                self._path.unlink()
        except Exception:
            pass

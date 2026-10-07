"""PidFile: writes on enter, removes on exit, guards against double-start."""
import os
import pathlib

import pytest

from shared.pidfile import PidFile, _pid_alive


def test_writes_and_removes(tmp_path: pathlib.Path):
    with PidFile("eb-test", directory=tmp_path) as pf:
        assert pf.path.exists()
        assert pf.path.read_text().strip() == str(os.getpid())
    assert not pf.path.exists(), "pidfile should be cleaned up on exit"


def test_stale_pidfile_is_reclaimed(tmp_path: pathlib.Path):
    # A pid that will definitely not be alive
    (tmp_path / "eb-test.pid").write_text("999999\n")
    with PidFile("eb-test", directory=tmp_path) as pf:
        assert pf.path.read_text().strip() == str(os.getpid())


def test_live_pidfile_refuses_start(tmp_path: pathlib.Path, monkeypatch):
    # Our parent: alive, and -- unlike our own pid -- not us. A pidfile naming our own
    # pid is deliberately reclaimable (see the PID 1 test below), so it cannot stand in
    # for "another process is running" here.
    (tmp_path / "eb-test.pid").write_text(f"{os.getppid()}\n")
    monkeypatch.delenv("RUN_FORCE", raising=False)
    with pytest.raises(SystemExit) as exc:
        with PidFile("eb-test", directory=tmp_path):
            pass
    assert "already running" in str(exc.value)
    # Pidfile must remain untouched so the running process's cleanup still works
    assert (tmp_path / "eb-test.pid").read_text().strip() == str(os.getppid())


def test_run_force_overrides(tmp_path: pathlib.Path, monkeypatch):
    (tmp_path / "eb-test.pid").write_text(f"{os.getppid()}\n")
    monkeypatch.setenv("RUN_FORCE", "1")
    with PidFile("eb-test", directory=tmp_path) as pf:
        assert pf.path.read_text().strip() == str(os.getpid())


def test_a_pidfile_naming_our_own_pid_is_reclaimed_not_refused(
        tmp_path: pathlib.Path, monkeypatch):
    """The container crash-loop: in a pod the restarted process is PID 1 again.

    An abrupt exit (OOMKill at the 512Mi limit, SIGKILL, node failure) leaves a pidfile
    naming 1 on the mounted volume -- both images set TMPDIR=/data -- and on restart
    `_pid_alive(1)` is True because PID 1 is the caller itself. The guard then refused
    to start forever, and on the demo overlay's PVC deleting the pod did not help.

    Our own PID stands in for PID 1: the property that matters is that the pidfile names
    the process doing the check, which cannot be a different live process.
    Reported by @huang195 during review of rossoctl/rossoctl#2609.
    """
    (tmp_path / "eb-test.pid").write_text(f"{os.getpid()}\n")
    monkeypatch.delenv("RUN_FORCE", raising=False)
    with PidFile("eb-test", directory=tmp_path) as pf:
        assert pf.path.read_text().strip() == str(os.getpid())
    assert not pf.path.exists(), "and normal cleanup still applies afterwards"


def test_pid_alive_zero_and_negative_are_dead():
    assert not _pid_alive(0)
    assert not _pid_alive(-1)


def test_pid_alive_self_is_true():
    assert _pid_alive(os.getpid())

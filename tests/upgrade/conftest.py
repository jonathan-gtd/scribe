"""Fixtures for the upgrade tests: a database filled by an older release.

Every other test starts from an empty database. Existing users do not: they
update with a database an older release wrote. For each release, the
`older_database` fixture creates a fresh database, checks the release's tag out
in a git worktree and runs `seed.py` there in a separate process — two versions
of `custom_components.scribe` cannot share one interpreter — so the old code
fills the database through its own setup path. The test then opens it with the
current code.

Needs the test TimescaleDB of docs/DEVELOPMENT.md, with a role allowed to
create databases, and the release tags (a shallow clone has none). Skips
otherwise; the CI job that runs these fails on a skip.
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import custom_components.scribe.writer  # noqa: F401  (patched below)
import psycopg2
import pytest

REPO = Path(__file__).resolve().parents[2]
ADMIN_DSN = os.environ.get(
    "SCRIBE_TEST_DSN", "postgresql://postgres:scribe@127.0.0.1:55432/scribe"
)
_STABLE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
# 3.0 and 3.1 only ever shipped as betas.
OLDEST = (3, 2)


def releases() -> list[str]:
    """The last patch of every stable minor since OLDEST, from the git tags.

    Every database layout a user can still be on. A release joins the list as
    soon as it is tagged.
    """
    try:
        tags = subprocess.run(
            ["git", "tag", "--list", "v*"],
            cwd=REPO,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.split()
    except (OSError, subprocess.CalledProcessError):
        return []
    latest: dict[tuple[int, int], tuple[int, str]] = {}
    for tag in tags:
        match = _STABLE_TAG.match(tag)
        if not match:
            continue
        major, minor, patch_level = map(int, match.groups())
        if (major, minor) < OLDEST:
            continue
        if (major, minor) not in latest or patch_level > latest[(major, minor)][0]:
            latest[(major, minor)] = (patch_level, tag)
    return [latest[key][1] for key in sorted(latest)]


def _dsn(database: str) -> str:
    return urlunsplit(urlsplit(ADMIN_DSN)._replace(path=f"/{database}"))


def _admin(sql: str, database: str | None = None):
    conn = psycopg2.connect(
        _dsn(database) if database else ADMIN_DSN, connect_timeout=3
    )
    conn.autocommit = True  # CREATE / DROP DATABASE cannot run in a transaction
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def mock_create_pool():
    """Override conftest's autouse mock: real asyncpg, minus idle timers."""
    real = asyncpg.create_pool

    def factory(*args, **kwargs):
        kwargs.setdefault("max_inactive_connection_lifetime", 0)
        return real(*args, **kwargs)

    with patch(
        "custom_components.scribe.writer.asyncpg.create_pool", side_effect=factory
    ):
        yield


@pytest.fixture
def older_database(release, socket_enabled, tmp_path):
    """A fresh database, filled by `release` with its own code. Yields its DSN."""
    try:
        _admin("SELECT 1")
    except psycopg2.OperationalError:
        pytest.skip(f"no TimescaleDB at {ADMIN_DSN}")

    database = "scribe_upgrade_" + re.sub(r"[^0-9a-z]", "_", release.lower())
    _admin(f'DROP DATABASE IF EXISTS "{database}"')
    _admin(f'CREATE DATABASE "{database}"')
    # As on a real installation: TimescaleDB is there before Scribe starts.
    _admin("CREATE EXTENSION IF NOT EXISTS timescaledb", database)

    worktree = tmp_path / release
    subprocess.run(
        ["git", "worktree", "add", "--detach", "--quiet", str(worktree), release],
        cwd=REPO,
        check=True,
    )
    try:
        seed = worktree / "upgrade_seed" / "test_seed.py"
        seed.parent.mkdir()
        shutil.copy(Path(__file__).with_name("seed.py"), seed)
        result = subprocess.run(
            [
                sys.executable, "-m", "pytest", str(seed),
                "-c", str(worktree / "pytest.ini"), "--rootdir", str(worktree),
                "-q", "-p", "no:cacheprovider",
            ],
            cwd=worktree,
            env={**os.environ, "SCRIBE_UPGRADE_DSN": _dsn(database)},
            capture_output=True,
            text=True,
        )  # fmt: skip
        if result.returncode:
            pytest.fail(
                f"{release} could not fill the database with its own code — the "
                f"older release failing, not a regression; raise OLDEST in "
                f"tests/upgrade/conftest.py if it can no longer run:\n"
                f"{result.stdout[-3000:]}"
            )
        yield _dsn(database)
    finally:
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(worktree)],
            cwd=REPO,
            check=False,
        )
        _admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')

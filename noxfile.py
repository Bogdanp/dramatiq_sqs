from functools import partial

from nox import Session
from nox_uv import session

session = partial(
    session,
    venv_backend="uv",
    uv_sync_locked=True,
)

PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]


@session(uv_only_groups=["dev"])
def ruff(session: Session) -> None:
    session.run("ruff", "check")
    session.run("ruff", "format", "--check")


@session(uv_groups=["dev"])
def ty(session: Session) -> None:
    session.run("ty", "check")


@session(uv_groups=["dev"], python=PYTHONS)
def pytest(session: Session) -> None:
    session.run("python", "-m", "pytest")

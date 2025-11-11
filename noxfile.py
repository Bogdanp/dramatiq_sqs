from nox import Session, options
from nox_uv import session

options.default_venv_backend = "uv"

PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]


@session(uv_only_groups=["dev"])
def ruff(session: Session) -> None:
    session.run("ruff", "check")
    session.run("ruff", "format", "--check")


@session(uv_groups=["dev"])
def mypy(session: Session) -> None:
    session.run("mypy", "src/", "tests/")


@session(uv_groups=["dev"], python=PYTHONS)
def pytest(session: Session) -> None:
    session.run("python", "-m", "pytest")

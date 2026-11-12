"""
Run dbt with the connection settings from .env.

dbt reads credentials from environment variables but does not read .env files
itself. This wrapper loads .env (real environment variables still win) and runs
dbt inside dbt_project/, so the same command works on macOS, Linux and Windows.

Usage:
    python scripts/dbt.py build
    python scripts/dbt.py build --full-refresh
    python scripts/dbt.py run --select fct_transactions
"""

import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.settings import REPO_ROOT, require  # noqa: E402  (importing loads .env)


def main():
    require("POSTGRES_PASSWORD")
    dbt = shutil.which("dbt")
    if dbt is None:
        sys.exit("dbt not found. Activate the virtualenv and run: pip install -r requirements.txt")
    sys.exit(subprocess.call([dbt, *sys.argv[1:]], cwd=REPO_ROOT / "dbt_project"))


if __name__ == "__main__":
    main()

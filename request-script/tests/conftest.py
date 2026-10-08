import os
import sys
from pathlib import Path

import pytest

REQUEST_SCRIPT_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = REQUEST_SCRIPT_DIR.parent

sys.path.insert(0, str(REQUEST_SCRIPT_DIR))


@pytest.fixture
def isolated_import():
    """Undo any previous import of load_data and restore the cwd afterwards.

    load_data.py reads its CSV at module level (line 52), so every test
    that cares about import-time behavior needs a clean, unimported module
    and must restore the original working directory it changes.
    """
    original_cwd = os.getcwd()
    sys.modules.pop("load_data", None)

    def _import():
        import load_data  # noqa: F401 (imported for its side effects)

        return load_data

    yield _import

    os.chdir(original_cwd)
    sys.modules.pop("load_data", None)

import sys
from pathlib import Path

# request-script/ is not a package; the DAG adds it to sys.path the same way.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "request-script"))

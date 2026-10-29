$ErrorActionPreference = "Stop"
python -m pip install pytest pyyaml
python -m pytest -q
python -m compileall -q application/flink/src
Push-Location application/flink/src
try { python simulation.py } finally { Pop-Location }

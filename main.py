"""
main.py
=======
Entrypoint script that orchestrates the full pipeline:
  1) Run the demographics crawler (Part1)
  2) Run the cleaning pipeline   (Part2)
  3) Run the feature engineering   (Part3)

Assumes the three scripts live alongside this file:
  demographics_crawler.py
  clean_df.py
  feature_engineering.py

Usage:
  python main.py
"""
import subprocess
import sys
from pathlib import Path

# Project root (this file's directory)
ROOT = Path(__file__).parent.resolve()

# Mapping of step name to script filename
STEPS = [
    ("Demographics crawling", "demographics_crawler.py"),
    ("Data cleaning",       "clean_df.py"),
    ("Feature engineering", "feature_engineering.py"),
]


def run_step(label: str, script: str) -> None:
    """
    Execute a Python script as a subprocess, raising if it fails.
    """
    script_path = ROOT / script
    if not script_path.exists():
        raise FileNotFoundError(f"Could not find script: {script_path}")
    print(f"\n=== Step: {label} ({script}) ===")
    subprocess.check_call([sys.executable, str(script_path)])


def main() -> None:
    """
    Run all pipeline steps in sequence.
    """
    for label, script in STEPS:
        run_step(label, script)
    print("\n✅ All steps completed successfully.")


if __name__ == "__main__":
    main()

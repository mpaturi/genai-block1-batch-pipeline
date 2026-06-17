"""Run the full Block 1 pipeline end-to-end: Synthea export, generator, and pipeline.

Usage:
    python scripts/run_all.py
    python scripts/run_all.py --population 500 --seed 1
"""

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JAR_PATH = PROJECT_ROOT / "tools" / "synthea-with-dependencies.jar"
OUTPUT_DIR = PROJECT_ROOT / "data" / "synthea_raw"


def _run(description, cmd):
    print(f"\n=== {description} ===")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"FAILED: {description} (exit code {result.returncode})")
        sys.exit(result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Run full Block 1 pipeline end-to-end")
    parser.add_argument("--population", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--reference-date", default="20250101")
    args = parser.parse_args()

    if not JAR_PATH.exists():
        print(f"ERROR: Synthea jar not found at '{JAR_PATH}'.")
        print("Download synthea-with-dependencies.jar from")
        print("https://github.com/synthetichealth/synthea/releases/latest")
        print("and place it there.")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    _run("Step 1/3: Running Synthea", [
        "java", "-jar", str(JAR_PATH),
        "-p", str(args.population),
        "-s", str(args.seed),
        "-r", args.reference_date,
        "--exporter.csv.export=true",
        "--exporter.fhir.export=false",
        f"--exporter.baseDirectory={OUTPUT_DIR}",
    ])

    _run("Step 2/3: Running generator (Synthea CSV -> data/raw/)", [
        sys.executable, "-m", "src.generator",
    ])

    _run("Step 3/3: Running pipeline (validate -> clean -> transform -> data/processed/)", [
        sys.executable, "-m", "src.pipeline",
    ])

    print("\n=== All steps completed successfully ===")


if __name__ == "__main__":
    main()

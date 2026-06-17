<#
.SYNOPSIS
Runs the full Block 1 pipeline end-to-end: Synthea export, generator, and pipeline.

.DESCRIPTION
Chains the three steps needed to reproduce the project output from scratch:
  1. scripts/run_synthea.ps1  — generate raw Synthea CSV into data/synthea_raw/
  2. python -m src.generator  — map Synthea CSV to simplified OMOP tables in data/raw/
  3. python -m src.pipeline   — validate, clean, transform, write data/processed/

All parameters are forwarded to run_synthea.ps1. Defaults match src/config.py so
a bare `./scripts/run_all.ps1` reproduces the canonical output.

Requires:
- Java 21 LTS on PATH (for Synthea)
- Python virtual environment activated with requirements.txt installed
- synthea-with-dependencies.jar at tools/synthea-with-dependencies.jar

.PARAMETER Population
Number of patients to generate. Default 10000.

.PARAMETER Seed
Random seed for reproducibility. Default 42.

.PARAMETER ReferenceDate
Reference date in yyyyMMdd format. Default 20250101.

.EXAMPLE
./scripts/run_all.ps1
.EXAMPLE
./scripts/run_all.ps1 -Population 500 -Seed 1
#>
param(
    [int]$Population = 10000,
    [int]$Seed = 42,
    [string]$ReferenceDate = "20250101"
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

Write-Host "`n=== Step 1/3: Running Synthea ===" -ForegroundColor Cyan
& "$scriptDir\run_synthea.ps1" -Population $Population -Seed $Seed -ReferenceDate $ReferenceDate

Write-Host "`n=== Step 2/3: Running generator (Synthea CSV -> data/raw/) ===" -ForegroundColor Cyan
python -m src.generator
if ($LASTEXITCODE -ne 0) { throw "Generator failed with exit code $LASTEXITCODE" }

Write-Host "`n=== Step 3/3: Running pipeline (validate -> clean -> transform -> data/processed/) ===" -ForegroundColor Cyan
python -m src.pipeline
if ($LASTEXITCODE -ne 0) { throw "Pipeline failed with exit code $LASTEXITCODE" }

Write-Host "`n=== All steps completed successfully ===" -ForegroundColor Green

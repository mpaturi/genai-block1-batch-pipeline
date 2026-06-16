<#
.SYNOPSIS
Runs the Synthea synthetic patient generator and exports its CSV output to data/synthea_raw/.

.DESCRIPTION
Wraps `java -jar synthea-with-dependencies.jar`, enabling the CSV exporter and
pointing it at data/synthea_raw/. Defaults for population, seed, and reference
date match src/config.py (NUM_PERSONS, RANDOM_SEED, REFERENCE_DATE) so the raw
export is reproducible.

Requires:
- Java 11+ on PATH
- synthea-with-dependencies.jar, downloaded from
  https://github.com/synthetichealth/synthea/releases/latest and placed at
  tools/synthea-with-dependencies.jar (or pass -JarPath).

.PARAMETER Population
Number of patients to generate. Default 10000 (matches NUM_PERSONS).

.PARAMETER Seed
Random seed for reproducibility. Default 42 (matches RANDOM_SEED).

.PARAMETER ReferenceDate
Reference date for the simulation, in yyyyMMdd format. Default 20250101
(matches REFERENCE_DATE).

.PARAMETER JarPath
Path to the downloaded synthea-with-dependencies.jar.

.PARAMETER OutputDir
Directory to write the Synthea CSV export to. Default data/synthea_raw.

.EXAMPLE
./scripts/run_synthea.ps1
.EXAMPLE
./scripts/run_synthea.ps1 -Population 500 -Seed 1
#>
param(
    [int]$Population = 10000,
    [int]$Seed = 42,
    [string]$ReferenceDate = "20250101",
    [string]$JarPath = "tools/synthea-with-dependencies.jar",
    [string]$OutputDir = "data/synthea_raw"
)

if (-not (Test-Path $JarPath)) {
    Write-Error "Synthea jar not found at '$JarPath'. Download synthea-with-dependencies.jar from https://github.com/synthetichealth/synthea/releases/latest and place it there, or pass -JarPath."
    exit 1
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

java -jar $JarPath `
    -p $Population `
    -s $Seed `
    -r $ReferenceDate `
    --exporter.csv.export=true `
    --exporter.fhir.export=false `
    --exporter.baseDirectory=$OutputDir

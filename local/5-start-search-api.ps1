#!/usr/bin/env pwsh
# Start Python Search API (Optional)

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Starting Python Search API" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$LocalDir = $PSScriptRoot
$WorkerDir = Join-Path (Split-Path $LocalDir -Parent) "worker"
Set-Location $WorkerDir

# Check if virtual environment exists
$VenvPath = Join-Path $WorkerDir "venv"
if (-not (Test-Path $VenvPath)) {
    Write-Host "Creating virtual environment..." -ForegroundColor Yellow
    python -m venv venv
}

# Activate virtual environment
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
$ActivateScript = Join-Path $VenvPath "Scripts\Activate.ps1"
& $ActivateScript

# Install dependencies
Write-Host "Installing/updating dependencies..." -ForegroundColor Yellow
pip install -r requirements.txt --quiet

# Load environment variables
Write-Host "`nLoading environment variables..." -ForegroundColor Yellow
$EnvFile = Join-Path $LocalDir ".env.worker"
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match "^\s*([^#][^=]+)\s*=\s*(.+)\s*$") {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
            Write-Host "  $key=$value" -ForegroundColor DarkGray
        }
    }
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "  Search API Running on :8000" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

# Add src directory to PYTHONPATH
$SrcDir = Join-Path $WorkerDir "src"
$env:PYTHONPATH = $SrcDir

uvicorn scene_seeker_worker.search_api:app --host 0.0.0.0 --port 8000

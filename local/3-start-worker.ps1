#!/usr/bin/env pwsh
# Start Python Worker

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Starting Python Worker" -ForegroundColor Cyan
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
pip install -r requirements.txt 

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
} else {
    Write-Host "Warning: .env.worker not found!" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "  Worker Running - Processing Jobs" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

# Add src directory to PYTHONPATH
$SrcDir = Join-Path $WorkerDir "src"
$env:PYTHONPATH = $SrcDir

python -m scene_seeker_worker.worker

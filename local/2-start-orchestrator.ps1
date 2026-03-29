#!/usr/bin/env pwsh
# Start Go Orchestrator

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Starting Go Orchestrator" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$LocalDir = $PSScriptRoot
$GoDir = Join-Path (Split-Path $LocalDir -Parent) "go"
Set-Location $GoDir

Write-Host "Loading environment variables..." -ForegroundColor Yellow
$EnvFile = Join-Path $LocalDir ".env.orchestrator"
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
    Write-Host "Warning: .env.orchestrator not found!" -ForegroundColor Red
}

Write-Host "`nDownloading Go dependencies..." -ForegroundColor Yellow
go mod download

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "  Orchestrator Running on :8080" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

go run ./cmd/orchestrator/main.go

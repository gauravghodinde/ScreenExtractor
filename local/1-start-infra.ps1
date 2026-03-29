#!/usr/bin/env pwsh
# Start Infrastructure Services (PostgreSQL, Redis, MinIO)

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Starting Infrastructure Services" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$LocalDir = $PSScriptRoot
Set-Location $LocalDir

Write-Host "Starting Docker containers..." -ForegroundColor Green
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nFailed to start infrastructure services!" -ForegroundColor Red
    exit 1
}

Write-Host "`nWaiting for services to be healthy..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "  Infrastructure Services Started" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`n  Services:" -ForegroundColor White
Write-Host "    PostgreSQL:  localhost:5433" -ForegroundColor Cyan
Write-Host "    Redis:       localhost:6380" -ForegroundColor Cyan
Write-Host "    MinIO:       localhost:9000" -ForegroundColor Cyan
Write-Host "    MinIO UI:    localhost:9001" -ForegroundColor Cyan
Write-Host "    PgAdmin:     localhost:5050" -ForegroundColor Cyan
Write-Host "`n  Credentials:" -ForegroundColor White
Write-Host "    MinIO:   minioadmin / minioadmin" -ForegroundColor DarkGray
Write-Host "    PgAdmin: admin@example.com / admin" -ForegroundColor DarkGray
Write-Host "    DB:      scene_user / scene_pass" -ForegroundColor DarkGray
Write-Host "`n========================================`n" -ForegroundColor Green

Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  Run .\2-start-orchestrator.ps1 in a new terminal" -ForegroundColor White
Write-Host "  Run .\3-start-worker.ps1 in a new terminal" -ForegroundColor White
Write-Host "  Run .\4-start-frontend.ps1 in a new terminal`n" -ForegroundColor White

#!/usr/bin/env pwsh
# Stop all services

Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "  Stopping All Services" -ForegroundColor Yellow
Write-Host "========================================`n" -ForegroundColor Yellow

$LocalDir = $PSScriptRoot
Set-Location $LocalDir

Write-Host "Stopping Docker infrastructure..." -ForegroundColor Yellow
docker-compose down

Write-Host "`nInfrastructure services stopped." -ForegroundColor Green
Write-Host "Press Ctrl+C in each terminal to stop application services.`n" -ForegroundColor White

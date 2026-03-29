#!/usr/bin/env pwsh
# Start Next.js Frontend

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Starting Next.js Frontend" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$LocalDir = $PSScriptRoot
$FrontendDir = Join-Path (Split-Path $LocalDir -Parent) "frontend"
Set-Location $FrontendDir

# Check if node_modules exists
if (-not (Test-Path "node_modules")) {
    Write-Host "Installing npm dependencies..." -ForegroundColor Yellow
    npm install
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "  Frontend Starting on :3000" -ForegroundColor Green
Write-Host "========================================`n" -ForegroundColor Green

npm run dev

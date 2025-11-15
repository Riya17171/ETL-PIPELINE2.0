# ETL Pipeline - Flask Web Server Starter (PowerShell)
# Run this to start the Flask server

Clear-Host
Write-Host "`n╔════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║      ETL Pipeline - Flask Web Server Starting...          ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════╝`n" -ForegroundColor Cyan

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
Set-Location $scriptDir

Write-Host "📂 Current directory: $(Get-Location)" -ForegroundColor Yellow
Write-Host ""

# Check if Flask is installed
Write-Host "📦 Checking dependencies..." -ForegroundColor Yellow
try {
    $flaskCheck = python -m pip list 2>$null | Select-String "flask"
    if ($null -eq $flaskCheck) {
        Write-Host "⚠️  Flask not installed. Installing now..." -ForegroundColor Yellow
        Write-Host ""
        python -m pip install -r requirement.txt
        Write-Host ""
    }
} catch {
    Write-Host "⚠️  Could not check Flask installation" -ForegroundColor Yellow
}

Write-Host "🚀 Starting Flask server..." -ForegroundColor Green
Write-Host ""
Write-Host "🌐 Web Interface: http://localhost:5000" -ForegroundColor Cyan
Write-Host "📡 API Ready for requests" -ForegroundColor Cyan
Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Gray
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host ""

# Start Flask server
python app.py

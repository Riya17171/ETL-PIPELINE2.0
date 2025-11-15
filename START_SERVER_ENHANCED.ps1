# ETL Pipeline - Flask Web Server Starter (PowerShell Version)
# Finds Python and starts the Flask server

Write-Host ""
Write-Host "╔════════════════════════════════════════════════════════════╗"
Write-Host "║      ETL Pipeline - Flask Web Server                       ║"
Write-Host "╚════════════════════════════════════════════════════════════╝"
Write-Host ""

# Check if Python exists
Write-Host "🔍 Checking for Python installation..."
Write-Host ""

$pythonPaths = @(
    "C:\Python312\python.exe",
    "C:\Python311\python.exe",
    "C:\Python310\python.exe",
    "C:\Program Files\Python312\python.exe",
    "C:\Program Files\Python311\python.exe",
    "C:\Program Files\Python310\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
    "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe"
)

$pythonExe = $null
foreach ($path in $pythonPaths) {
    if (Test-Path $path) {
        $pythonExe = $path
        Write-Host "✅ Found Python at: $path"
        & $pythonExe --version
        break
    }
}

if (-not $pythonExe) {
    Write-Host ""
    Write-Host "❌ ERROR: Python is not installed!"
    Write-Host ""
    Write-Host "HOW TO FIX:"
    Write-Host "  1. Visit: https://www.python.org/downloads/"
    Write-Host "  2. Download and run the installer"
    Write-Host "  3. ✓ Check 'Add Python to PATH'"
    Write-Host "  4. Complete installation"
    Write-Host "  5. Restart PowerShell"
    Write-Host "  6. Try again"
    Write-Host ""
    Write-Host "Opening download page..."
    Start-Process "https://www.python.org/downloads/"
    Read-Host "Press Enter after installing Python..."
    exit 1
}

Write-Host ""
Write-Host "📂 Current directory: $(Get-Location)"
Write-Host ""

# Check dependencies
Write-Host "📦 Checking dependencies..."
$output = & $pythonExe -m pip list 2>&1
if ($output -like "*flask*") {
    Write-Host "✅ All dependencies are installed"
} else {
    Write-Host "⚠️  Installing dependencies..."
    Write-Host ""
    & $pythonExe -m pip install --upgrade pip
    Write-Host ""
    & $pythonExe -m pip install -r requirement.txt
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "❌ Error installing dependencies!"
        Read-Host "Press Enter to exit..."
        exit 1
    }
}

Write-Host ""
Write-Host "🚀 Starting Flask server..."
Write-Host ""
Write-Host "🌐 Web Interface: http://localhost:5000"
Write-Host "📡 API Ready for requests"
Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
Write-Host "ℹ️  Press Ctrl+C to stop the server"
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
Write-Host ""

& $pythonExe app.py

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "❌ Error starting Flask server!"
    Read-Host "Press Enter to exit..."
    exit 1
}

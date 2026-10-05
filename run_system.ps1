Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "            Starting ResellRadar System                 " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[1/2] Starting Pipeline Backend Server (Port 8000)..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python server.py"

Start-Sleep -Seconds 2

Write-Host "[2/2] Starting Monitoring Dashboard (Port 3000)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd dashboard; npm run dev"

Write-Host ""
Write-Host "Both services are launching in dedicated consoles:" -ForegroundColor Cyan
Write-Host "  - Frontend Dashboard: http://localhost:3000" -ForegroundColor White
Write-Host "  - Backend API Server: http://127.0.0.1:8000" -ForegroundColor White
Write-Host ""
Write-Host "Navigate to http://localhost:3000 and click 'Start pipeline' to run all stages!" -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Cyan

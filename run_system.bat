@echo off
echo ========================================================
echo        Starting ResellRadar System
echo ========================================================
echo.
echo [1/2] Starting Pipeline Backend Server (Port 8000)...
start "ResellRadar - Backend Server" cmd /k "python server.py"

timeout /t 2 /nobreak >nul

echo [2/2] Starting Monitoring Dashboard (Port 3000)...
start "ResellRadar - Frontend Dashboard" cmd /k "cd dashboard && npm run dev"

echo.
echo All services launched!
echo - Dashboard:  http://localhost:3000
echo - API Server: http://127.0.0.1:8000
echo.
echo Click 'Start pipeline' in the dashboard to execute the pipeline!
echo ========================================================

@echo off
echo Starting CLEXER V6.0...
start "" "C:\Program Files\WindowsApps\TradingView.Desktop_3.2.0.7916_x64__n534cwy3pjxzj\TradingView.exe" --remote-debugging-port=9222
echo TradingView starting...
timeout /t 8 /nobreak
start "" cmd /k "python C:\Users\bhaba\Downloads\tv_bridge.py"
echo Bridge starting...
timeout /t 5 /nobreak
start "" cmd /k "ngrok http 8765"
echo Ngrok starting...
echo.
echo All 3 started! Check CMD windows.
echo Remember to update TV_BRIDGE_URL in Railway if ngrok URL changed.
timeout /t 5
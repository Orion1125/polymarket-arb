@echo off
cd /d "%~dp0"
powershell -NoExit -Command "Set-Location '%~dp0'; python .\polymarket_paper_trader.py --mode loop"

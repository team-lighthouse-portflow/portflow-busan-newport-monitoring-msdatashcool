@echo off
cd /d C:\Users\EL053\Documents\busan-port-pipeline
echo [현재 폴더] %CD%
echo [python 버전 확인]
.venv\Scripts\python.exe --version
echo.
echo [파이프라인 실행]
.venv\Scripts\python.exe run_pipeline.py
echo.
echo [종료코드] %ERRORLEVEL%
pause
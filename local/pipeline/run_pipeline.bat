@echo off
cd /d C:\Users\EL053\Documents\busan-port-pipeline
if not exist logs mkdir logs
.venv\Scripts\python.exe run_pipeline.py >> logs\scheduler_console.log 2>&1
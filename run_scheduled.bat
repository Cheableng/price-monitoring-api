@echo off 
cd /d C:\Users\ADMIN\Desktop\price-monitoring-system 
call venv\Scripts\activate 
:loop 
python scripts\run_pipeline.py 
timeout /t 600 
goto loop 

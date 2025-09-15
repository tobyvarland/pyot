@echo off
setlocal

set SERVICE=PyOT
for %%i in ("%~dp0..") do set "FOLDER=%%~fi"

if not exist "%FOLDER%\.venv\Scripts\python.exe" (
    "C:\Users\plant\AppData\Local\Programs\Python\Python313\python.exe" -m venv "%FOLDER%\.venv"
)
if exist "%FOLDER%\requirements.txt" (
  "%FOLDER%\.venv\Scripts\pip.exe" install --upgrade pip
  "%FOLDER%\.venv\Scripts\pip.exe" install -r "%FOLDER%\requirements.txt" || (echo Pip install failed.& exit /b 1)
)

for %%D in ("%FOLDER%\output" "%FOLDER%\logs" "C:\PLCData" "C:\PLCData\Charts" "C:\PLCData\Logs" "C:\PLCData\Recipes" "C:\PLCData\EventLog" "C:\PLCData\SHOPORDER") do mkdir "%%~D" 2>nul

if not exist "%FOLDER%\.env" (
    copy "%FOLDER%\.env.example" "%FOLDER%\.env" >nul
)
echo Ensure .env file up to date before continuing.
echo Ensure SSH key authentication is set up for any remote servers.
echo Also make sure to install any certs in the certs folder.
pause

"%FOLDER%\nssm.exe" install "%SERVICE%" "%FOLDER%\.venv\Scripts\python.exe" "%FOLDER%\pyot.py"
"%FOLDER%\nssm.exe" set "%SERVICE%" AppDirectory "%FOLDER%"
"%FOLDER%\nssm.exe" set "%SERVICE%" AppStdout "%FOLDER%\output\stdout.out"
"%FOLDER%\nssm.exe" set "%SERVICE%" AppStderr "%FOLDER%\output\stderr.out"
"%FOLDER%\nssm.exe" set "%SERVICE%" AppRotateFiles 1
"%FOLDER%\nssm.exe" set "%SERVICE%" AppRotateBytes 10485760
"%FOLDER%\nssm.exe" set "%SERVICE%" AppExit Default Restart
"%FOLDER%\nssm.exe" set "%SERVICE%" AppRestartDelay 5000
"%FOLDER%\nssm.exe" set "%SERVICE%" ObjectName ".\plant" "plant"

sc.exe config "%SERVICE%" depend= Tcpip/LanmanWorkstation/WSLService
sc.exe config "%SERVICE%" start= delayed-auto

"%FOLDER%\nssm.exe" start "%SERVICE%"

endlocal
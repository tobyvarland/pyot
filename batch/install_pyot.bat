@echo off
setlocal

set SERVICE=PyOT
for %%i in ("%~dp0..") do set "FOLDER=%%~fi"

mkdir "%FOLDER%\output" 2>nul
mkdir "%FOLDER%\logs" 2>nul

"%FOLDER%\nssm.exe" install "%SERVICE%" "%FOLDER%\.venv\Scripts\python.exe" "%FOLDER%\pyot.py"
"%FOLDER%\nssm.exe" set %SERVICE% AppDirectory "%FOLDER%"
"%FOLDER%\nssm.exe" set %SERVICE% AppStdout "%FOLDER%\output\stdout.log"
"%FOLDER%\nssm.exe" set %SERVICE% AppStderr "%FOLDER%\output\stderr.log"
"%FOLDER%\nssm.exe" set %SERVICE% AppRotateFiles 1
"%FOLDER%\nssm.exe" set %SERVICE% AppRotateBytes 10485760
"%FOLDER%\nssm.exe" set %SERVICE% AppExit Default Restart
"%FOLDER%\nssm.exe" set %SERVICE% AppRestartDelay 5000
"%FOLDER%\nssm.exe" set %SERVICE% ObjectName ".\plant" "plant"

sc.exe config %SERVICE% depend= Tcpip/LanmanWorkstation/WSLService
sc.exe config %SERVICE% start= delayed-auto

%FOLDER%\nssm.exe start %SERVICE%

endlocal
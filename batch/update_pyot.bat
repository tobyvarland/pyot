@echo off
setlocal

set SERVICE=PyOT
for %%i in ("%~dp0..") do set "FOLDER=%%~fi"

C:\Users\plant\AppData\Local\Programs\Git\cmd\git.exe config --global --add safe.directory "%FOLDER%"
C:\Users\plant\AppData\Local\Programs\Git\cmd\git.exe -C "%FOLDER%" fetch
C:\Users\plant\AppData\Local\Programs\Git\cmd\git.exe -C "%FOLDER%" pull

if exist "%FOLDER%\requirements.txt" (
  "%FOLDER%\.venv\Scripts\pip.exe" install --upgrade pip
  "%FOLDER%\.venv\Scripts\pip.exe" install -r "%FOLDER%\requirements.txt" || (echo Pip install failed.& exit /b 1)
)

echo Make any required config changes first before service restarted.
pause

"%FOLDER%\nssm.exe" restart "%SERVICE%"

pause

endlocal
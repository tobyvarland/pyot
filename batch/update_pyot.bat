@echo off
setlocal

set SERVICE=PyOT
for %%i in ("%~dp0..") do set "FOLDER=%%~fi"

git fetch && git pull

if exist "%FOLDER%\requirements.txt" (
  "%FOLDER%\.venv\Scripts\pip.exe" install --upgrade pip
  "%FOLDER%\.venv\Scripts\pip.exe" install -r "%FOLDER%\requirements.txt" || (echo Pip install failed.& exit /b 1)
)

echo Must manually restart the service to apply updates.
pause

endlocal
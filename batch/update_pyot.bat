@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem --- Settings/paths ---
set "SERVICE=PyOT"
for %%i in ("%~dp0..") do set "FOLDER=%%~fi"

rem --- Git update (safe directory, fetch, pull) ---
"C:\Users\plant\AppData\Local\Programs\Git\cmd\git.exe" config --global --add safe.directory "%FOLDER%"
"C:\Users\plant\AppData\Local\Programs\Git\cmd\git.exe" -C "%FOLDER%" fetch
"C:\Users\plant\AppData\Local\Programs\Git\cmd\git.exe" -C "%FOLDER%" pull

rem --- Python deps (if requirements.txt exists) ---
if exist "%FOLDER%\requirements.txt" (
  rem "%FOLDER%\.venv\Scripts\pip.exe" install --upgrade pip
  "%FOLDER%\.venv\Scripts\pip.exe" install -r "%FOLDER%\requirements.txt" || (echo Pip install failed.& exit /b 1)
)

rem --- Environment file update ---
set "ENVFILE=%FOLDER%\.env"
set "TEMPLATE=%FOLDER%\.env.example"
set "NEED_NEWLINE_DONE="

if not exist "%TEMPLATE%" (
  echo ERROR: "%TEMPLATE%" not found.
  exit /b 1
)

if not exist "%ENVFILE%" (
  echo Creating new "%ENVFILE%"...
  type nul > "%ENVFILE%"
)

for /f "usebackq tokens=1* delims==" %%A in ("%TEMPLATE%") do (
  set "key=%%~A"
  set "default=%%~B"

  for /f "tokens=* delims= " %%K in ("!key!") do set "key=%%K"

  if defined key (
    set "first=!key:~0,1!"
    if /I not "!first!"=="#" if /I not "!first!"==";" (
      >nul findstr /B /I /C:"!key!=" "%ENVFILE%"
      if errorlevel 1 (
        echo(
        echo Missing key: !key!
        if defined default (
          echo Template default: "!default!"
          <nul set /p "=Enter value for !key! [press Enter to use default]: "
        ) else (
          <nul set /p "=Enter value for !key!: "
        )

        set "value="
        set /p "value="
        if not defined value set "value=!default!"

        rem Ensure a single leading newline if .env is non-empty
        if not defined NEED_NEWLINE_DONE (
          for %%F in ("%ENVFILE%") do if %%~zF NEQ 0 >>"%ENVFILE%" echo.
          set "NEED_NEWLINE_DONE=1"
        )

        rem Escape literal exclamation marks so they survive delayed expansion
        set "safevalue=!value:^!=^^^!!"

        rem Write without echo to avoid issues with &, >, |, etc.
        >>"%ENVFILE%" <nul set /p "=!key!=!safevalue!"
        >>"%ENVFILE%" echo.
      )
    )
  )
)

echo(
echo Environment updated. Double check configuration if necessary before service restart.
pause

rem --- Restart service via NSSM ---
"%FOLDER%\nssm.exe" restart "%SERVICE%"

pause
endlocal
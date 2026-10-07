@echo off
title Kpop Girls Telegram Bot
cd /d "%~dp0"

echo ===================================================
echo             KPOP GIRLS TELEGRAM BOT
echo ===================================================
echo.

rem Menghapus file PID lama jika ada
if exist bot.pid (
    echo Menemukan file bot.pid lama. Mencoba membersihkan...
    del bot.pid
)

rem Deteksi Virtual Environment (venv atau .venv)
set "VENV_ACTIVE="

if exist venv\Scripts\activate.bat (
    echo Mengaktifkan Virtual Environment venv...
    call venv\Scripts\activate.bat
    set "VENV_ACTIVE=1"
)

if not defined VENV_ACTIVE (
    if exist .venv\Scripts\activate.bat (
        echo Mengaktifkan Virtual Environment .venv...
        call .venv\Scripts\activate.bat
        set "VENV_ACTIVE=1"
    )
)

if not defined VENV_ACTIVE (
    echo Virtual Environment tidak ditemukan. Menjalankan menggunakan Python global...
)

echo Menjalankan bot (main.py)...
echo Tekan Ctrl+C untuk menghentikan bot.
echo.

python main.py

echo.
echo Bot telah berhenti.
pause

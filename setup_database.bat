@echo off
echo 🚀 AcadExtract Database Setup
echo ========================================

echo 1️⃣ Checking PostgreSQL service...
sc query postgresql-x64-16 | find "RUNNING" > nul
if %errorlevel% equ 0 (
    echo ✅ PostgreSQL is already running
) else (
    echo ❌ PostgreSQL is not running
    echo.
    echo 🔄 Starting PostgreSQL service...
    net start postgresql-x64-16
    timeout /t 10
    sc query postgresql-x64-16 | find "RUNNING" > nul
    if %errorlevel% equ 0 (
        echo ✅ PostgreSQL started successfully
    ) else (
        echo ❌ Failed to start PostgreSQL
        echo.
        echo 💡 Please install PostgreSQL from: https://www.postgresql.org/download/windows/
        pause
        exit /b 1
    )
)

echo.
echo 2️⃣ Creating database if needed...
psql -h localhost -p 5434 -U postgres -c "CREATE DATABASE email_agent;" 2>nul
if %errorlevel% equ 0 (
    echo ✅ Database exists or created successfully
) else (
    echo ⚠️  Database creation failed - may already exist
)

echo.
echo 3️⃣ Running schema setup...
psql -h localhost -p 5434 -U postgres -d email_agent -f schemas\001_core_schema.sql
if %errorlevel% equ 0 (
    echo ✅ Core schema applied
) else (
    echo ❌ Core schema failed
)

psql -h localhost -p 5434 -U postgres -d email_agent -f schemas\002_app_partitions.sql
if %errorlevel% equ 0 (
    echo ✅ Partition schema applied
) else (
    echo ❌ Partition schema failed
)

echo.
echo 4️⃣ Initializing database...
python -c "from src.common.database import init_db; print('Database UUID:', init_db())"

echo.
echo ✅ Database setup complete!
echo 🚀 You can now start the AcadExtract system:
echo    python -m uvicorn src.api.app:app --host 127.0.0.1 --port 8002 --reload
echo.
pause

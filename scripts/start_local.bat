@echo off
REM ============================================================
REM VinaMilk Data Lakehouse - Windows Startup Script
REM ============================================================
SETLOCAL

echo.
echo ========================================================
echo  VinaMilk Data Lakehouse — Local Environment Setup
echo  Stack: PostgreSQL + Kafka + Debezium + Schema Registry
echo ========================================================
echo.

REM Step 1: Start all Docker services
echo [1/4] Starting Docker services...
docker compose up -d
IF ERRORLEVEL 1 (
    echo ERROR: docker compose failed. Is Docker Desktop running?
    pause & exit /b 1
)

REM Step 2: Wait for services to be healthy
echo.
echo [2/4] Waiting for services to become healthy (60s)...
timeout /t 60 /nobreak > nul

REM Step 3: Register Debezium connector
echo.
echo [3/4] Registering Debezium PostgreSQL connector...
:retry_debezium
curl -s http://localhost:8083/connectors > nul 2>&1
IF ERRORLEVEL 1 (
    echo   Debezium not ready yet, waiting 10s...
    timeout /t 10 /nobreak > nul
    GOTO retry_debezium
)

curl -s -X POST http://localhost:8083/connectors ^
  -H "Content-Type: application/json" ^
  -d @connectors\debezium-postgres-connector.json
echo.

REM Verify connector status
echo.
echo Connector status:
curl -s http://localhost:8083/connectors/vinamik-postgres-connector/status
echo.

REM Step 4: Install Python dependencies
echo.
echo [4/4] Installing Python dependencies...
pip install -q -r data_generator\requirements.txt
pip install -q -r data_generator\requirements.txt 2>nul

echo.
echo ========================================================
echo  All services started! Access points:
echo    Kafka UI:      http://localhost:8080
echo    Schema Registry: http://localhost:8081
echo    Debezium REST: http://localhost:8083
echo    PostgreSQL:    localhost:5432 (vinamik_erp)
echo.
echo  Start data generation:
echo    python data_generator\generate_erp_data.py --speed normal
echo.
echo  Start MISA producer:
echo    python producers\misa_csv_producer.py --loop
echo.
echo  Start FX rate producer:
echo    python producers\fx_rate_producer.py --loop --mock
echo ========================================================
echo.

ENDLOCAL

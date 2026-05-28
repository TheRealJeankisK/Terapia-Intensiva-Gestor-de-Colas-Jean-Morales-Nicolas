@echo off
title Iniciar Proyecto RabbitMQ UCI
echo ============================================================
echo   Iniciador Automático del Proyecto RabbitMQ UCI
echo ============================================================
echo.

:: 1. Install requirements
echo [1/3] Verificando e instalando dependencias (pika)...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo [ADVERTENCIA] Hubo un problema al ejecutar pip. Asegúrese de tener Python y pip instalados en el PATH.
)
echo.

:: 2. Configure Virtual Host inside Docker container
echo [2/3] Intentando configurar el Virtual Host '/uci_app' en el contenedor 'rabbitmq-uci'...
docker exec -it rabbitmq-uci rabbitmqctl add_vhost /uci_app
docker exec -it rabbitmq-uci rabbitmqctl set_permissions -p /uci_app guest ".*" ".*" ".*"
echo.
echo [INFO] Si usas la versión nativa de RabbitMQ (sin Docker), asegúrate de crear
echo        el Virtual Host '/uci_app' manualmente en http://localhost:15672/
echo.

:: 3. Launch Python scripts in separate CMD windows
echo [3/3] Iniciando el Productor y los Consumidores en ventanas independientes...
start "Consumidor Medico (Clínico)" cmd /k "python consumidor_medico.py"
timeout /t 1 >nul
start "Consumidor Seguridad (Operaciones)" cmd /k "python consumidor_seguridad.py"
timeout /t 1 >nul
start "Productor UCI (Simulador)" cmd /k "python productor_uci.py"

echo.
echo [ÉXITO] ¡Todo ha sido iniciado! Revisa las tres nuevas ventanas de comandos.
echo.
pause

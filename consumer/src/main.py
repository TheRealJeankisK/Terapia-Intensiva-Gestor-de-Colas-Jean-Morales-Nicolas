import logging
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List

from src.domain.models import TelemetryAlert
from src.domain.ports import AlertRepository, MessageConsumer
from src.infrastructure.db_adapter import SQLiteAlertRepository
from src.infrastructure.broker_adapter import RabbitMQConsumer

# Configure structured logging in English
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("consumer")

# Instantiate port adapters
db_repository: AlertRepository = SQLiteAlertRepository()
message_consumer: MessageConsumer = RabbitMQConsumer()

# Setup templates directory for live dashboard rendering
templates = Jinja2Templates(directory="src/templates")

def handle_incoming_alert(alert: TelemetryAlert) -> None:
    """
    Callback executed when a message is successfully received from the queue.
    Persists the alert in the local SQLite consumer database.
    """
    logger.info(f"Callback invoked: Persisting alert {alert.alert_id} for patient {alert.patient_name}")
    try:
        db_repository.save(alert)
    except Exception as e:
        logger.error(f"Error persisting consumed alert to database: {str(e)}")
        # Raise exception to prompt RabbitMQ consumer to nack/requeue the message
        raise e

# Background consumer thread variable
consumer_thread = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles background thread execution for message consumption on startup,
    and terminates connections on shutdown.
    """
    global consumer_thread
    logger.info("Telemetry Consumer microservice starting up...")
    
    # Start RabbitMQ consumer loop in a separate daemon thread to avoid blocking FastAPI
    consumer_thread = threading.Thread(
        target=message_consumer.start_consuming,
        args=(handle_incoming_alert,),
        daemon=True
    )
    consumer_thread.start()
    logger.info("Background RabbitMQ consumer thread started.")
    
    yield
    
    logger.info("Telemetry Consumer microservice shutting down...")
    message_consumer.stop_consuming()

app = FastAPI(
    title="Medical Telemetry Consumer API",
    description="Microservice responsible for listening to vital alert updates, persisting them, and exposing consumed metrics.",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/", response_class=HTMLResponse)
def serve_dashboard(request: Request):
    """
    Serves the live interactive dark-mode dashboard HTML page.
    """
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/alerts/consumed", response_model=List[TelemetryAlert])
def get_consumed_alerts():
    """
    Exposes the list of consumed alerts stored in the database.
    This fulfills the requirement of exposing consumed messages via API.
    """
    return db_repository.get_all()

@app.get("/health")
def health_check():
    """
    Checks DB connectivity and background consumer connection status.
    """
    db_ok = True
    
    # Check DB
    try:
        db_repository.get_all()
    except Exception:
        db_ok = False
        
    # Check if consumer thread is active and connected
    consumer_running = consumer_thread is not None and consumer_thread.is_alive()
    
    if db_ok and consumer_running:
        return {"status": "healthy", "database": "connected", "broker_listener": "active"}
    else:
        return {
            "status": "unhealthy",
            "database": "connected" if db_ok else "disconnected",
            "broker_listener": "active" if consumer_running else "inactive"
        }

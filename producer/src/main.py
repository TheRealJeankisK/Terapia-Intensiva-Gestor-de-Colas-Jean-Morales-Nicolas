import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from typing import List

from src.domain.models import TelemetryAlertCreate, TelemetryAlert
from src.domain.ports import AlertRepository, MessagePublisher
from src.infrastructure.db_adapter import SQLiteAlertRepository
from src.infrastructure.broker_adapter import RabbitMQPublisher

# Configure structured logging in English
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("producer")

# Instantiating port adapters
# Using dependency injection patterns conceptually
db_repository: AlertRepository = SQLiteAlertRepository()
message_publisher: MessagePublisher = RabbitMQPublisher()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles startup and shutdown events to clean up resources gracefully.
    """
    logger.info("Telemetry Producer microservice starting up...")
    yield
    logger.info("Telemetry Producer microservice shutting down...")
    message_publisher.close()

app = FastAPI(
    title="Medical Telemetry Producer API",
    description="Microservice responsible for validating, persisting, and dispatching patient clinical alerts.",
    version="1.0.0",
    lifespan=lifespan
)

@app.post("/alerts/send", status_code=status.HTTP_201_CREATED)
def send_telemetry_alert(alert_input: TelemetryAlertCreate):
    """
    Receives patient vital signs, persists a log in the local SQLite DB,
    and publishes the alert asynchronously to the RabbitMQ queue.
    """
    # 1. Instantiate the Domain model with default generated ID and status='pending'
    domain_alert = TelemetryAlert(
        patient_id=alert_input.patient_id,
        patient_name=alert_input.patient_name,
        heart_rate=alert_input.heart_rate,
        blood_pressure=alert_input.blood_pressure,
        alert_level=alert_input.alert_level.upper()
    )
    
    logger.info(f"Received alert for patient {domain_alert.patient_id} ({domain_alert.patient_name})")
    
    # 2. Persist in the local SQLite Database (almacenamiento propio)
    try:
        db_repository.save(domain_alert)
    except Exception as e:
        logger.error(f"Local storage failure: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to persist alert in local storage"
        )
        
    # 3. Publish asynchronously to the queue (buzón de mensajes)
    publish_success = message_publisher.publish(domain_alert)
    
    # 4. Update local log status based on queue dispatch outcome
    if publish_success:
        db_repository.update_status(domain_alert.alert_id, "sent")
        return {
            "message": "Alert published and logged successfully",
            "alert_id": domain_alert.alert_id,
            "status": "sent"
        }
    else:
        db_repository.update_status(domain_alert.alert_id, "failed")
        logger.error(f"Dispatched status failed for alert {domain_alert.alert_id}")
        return {
            "message": "Alert logged locally but failed to publish asynchronously",
            "alert_id": domain_alert.alert_id,
            "status": "failed"
        }

@app.get("/alerts/history", response_model=List[TelemetryAlert])
def get_alert_history():
    """
    Returns the log history of all alerts processed by this producer from its SQLite database.
    """
    return db_repository.get_all()

@app.get("/health")
def health_check():
    """
    Validates microservice health by verifying SQLite and RabbitMQ connections.
    """
    db_ok = True
    broker_ok = True
    
    # Check DB
    try:
        db_repository.get_all()
    except Exception:
        db_ok = False
        
    # Check RabbitMQ
    try:
        broker_ok = message_publisher._connect()
    except Exception:
        broker_ok = False
        
    if db_ok and broker_ok:
        return {"status": "healthy", "database": "connected", "broker": "connected"}
    else:
        return {
            "status": "unhealthy",
            "database": "connected" if db_ok else "disconnected",
            "broker": "connected" if broker_ok else "disconnected"
        }

from datetime import datetime
from pydantic import BaseModel, Field

class TelemetryAlert(BaseModel):
    """
    Domain model representing a consumed patient telemetry alert.
    Includes the timestamp when the alert was published and the received_at timestamp when consumed.
    """
    alert_id: str
    patient_id: str
    patient_name: str
    heart_rate: int
    blood_pressure: str
    alert_level: str
    timestamp: datetime
    received_at: datetime = Field(default_factory=datetime.utcnow)

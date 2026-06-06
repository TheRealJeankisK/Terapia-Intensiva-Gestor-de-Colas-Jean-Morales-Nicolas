from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel, Field
from typing import Optional

class TelemetryAlertCreate(BaseModel):
    """
    Schema for incoming telemetry alert requests.
    Validates patient ID, heart rate, blood pressure, and clinical alert level.
    """
    patient_id: str = Field(..., description="Unique code identifying the patient")
    patient_name: str = Field(..., description="Full name of the patient")
    heart_rate: int = Field(..., description="Heart rate in beats per minute (bpm)", ge=0)
    blood_pressure: str = Field(..., description="Blood pressure values, e.g. '120/80'")
    alert_level: str = Field(..., description="Alert severity level: INFO, WARNING, CRITICAL")

class TelemetryAlert(BaseModel):
    """
    Core Domain model representing a telemetry alert log inside the Producer microservice.
    """
    alert_id: str = Field(default_factory=lambda: str(uuid4()))
    patient_id: str
    patient_name: str
    heart_rate: int
    blood_pressure: str
    alert_level: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = "pending"  # sent, failed, pending

import logging
from typing import List
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from src.config import settings
from src.domain.models import TelemetryAlert
from src.domain.ports import AlertRepository

# Setup module logger
logger = logging.getLogger(__name__)

Base = declarative_base()

class SQLConsumedAlert(Base):
    """
    SQLAlchemy Model for the consumed telemetry alert table.
    """
    __tablename__ = "consumed_alerts"

    alert_id = Column(String, primary_key=True)
    patient_id = Column(String, nullable=False)
    patient_name = Column(String, nullable=False)
    heart_rate = Column(Integer, nullable=False)
    blood_pressure = Column(String, nullable=False)
    alert_level = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    received_at = Column(DateTime, nullable=False)


class SQLiteAlertRepository(AlertRepository):
    """
    SQLAlchemy repository implementation adhering to the Consumer's AlertRepository Port.
    """
    def __init__(self, database_url: str = settings.DATABASE_URL):
        connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
        self.engine = create_engine(database_url, connect_args=connect_args)
        
        # Auto-create tables
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        logger.info("SQLite Consumer Database initialized and tables verified.")

    def save(self, alert: TelemetryAlert) -> TelemetryAlert:
        session = self.SessionLocal()
        try:
            sql_alert = SQLConsumedAlert(
                alert_id=alert.alert_id,
                patient_id=alert.patient_id,
                patient_name=alert.patient_name,
                heart_rate=alert.heart_rate,
                blood_pressure=alert.blood_pressure,
                alert_level=alert.alert_level,
                timestamp=alert.timestamp,
                received_at=alert.received_at
            )
            session.add(sql_alert)
            session.commit()
            logger.info(f"Consumed alert {alert.alert_id} successfully stored in local database.")
            return alert
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to store consumed alert {alert.alert_id} in database: {str(e)}")
            raise e
        finally:
            session.close()

    def get_all(self) -> List[TelemetryAlert]:
        session = self.SessionLocal()
        try:
            # Query and order by received_at descending so recent alerts are first on dashboard
            sql_alerts = session.query(SQLConsumedAlert).order_by(SQLConsumedAlert.received_at.desc()).all()
            alerts = []
            for item in sql_alerts:
                alerts.append(TelemetryAlert(
                    alert_id=item.alert_id,
                    patient_id=item.patient_id,
                    patient_name=item.patient_name,
                    heart_rate=item.heart_rate,
                    blood_pressure=item.blood_pressure,
                    alert_level=item.alert_level,
                    timestamp=item.timestamp,
                    received_at=item.received_at
                ))
            return alerts
        except Exception as e:
            logger.error(f"Failed to query alerts from local database: {str(e)}")
            return []
        finally:
            session.close()

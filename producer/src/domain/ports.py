from abc import ABC, abstractmethod
from typing import List
from src.domain.models import TelemetryAlert

class AlertRepository(ABC):
    """
    Port (Interface) for database storage operations.
    """
    @abstractmethod
    def save(self, alert: TelemetryAlert) -> TelemetryAlert:
        """Saves a telemetry alert log."""
        pass

    @abstractmethod
    def get_all(self) -> List[TelemetryAlert]:
        """Retrieves all telemetry alert logs."""
        pass

    @abstractmethod
    def update_status(self, alert_id: str, status: str) -> None:
        """Updates the dispatch status of an alert (e.g., 'sent', 'failed')."""
        pass


class MessagePublisher(ABC):
    """
    Port (Interface) for the Message Queue broker operations.
    """
    @abstractmethod
    def publish(self, alert: TelemetryAlert) -> bool:
        """
        Publishes a telemetry alert to the message broker queue.
        Returns True if successful, False otherwise.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Closes connection channels to the broker gracefully."""
        pass

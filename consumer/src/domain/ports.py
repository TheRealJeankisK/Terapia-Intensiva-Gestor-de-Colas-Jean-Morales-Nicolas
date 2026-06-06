from abc import ABC, abstractmethod
from typing import List, Callable
from src.domain.models import TelemetryAlert

class AlertRepository(ABC):
    """
    Port (Interface) for consumer database storage operations.
    """
    @abstractmethod
    def save(self, alert: TelemetryAlert) -> TelemetryAlert:
        """Saves a consumed telemetry alert."""
        pass

    @abstractmethod
    def get_all(self) -> List[TelemetryAlert]:
        """Retrieves all consumed telemetry alerts stored in DB."""
        pass


class MessageConsumer(ABC):
    """
    Port (Interface) for the Message Queue broker listener operations.
    """
    @abstractmethod
    def start_consuming(self, on_message_callback: Callable[[TelemetryAlert], None]) -> None:
        """
        Starts listening to the message queue.
        Invokes the on_message_callback callable whenever a TelemetryAlert is consumed.
        """
        pass

    @abstractmethod
    def stop_consuming(self) -> None:
        """Stops consuming and closes connection channel gracefully."""
        pass

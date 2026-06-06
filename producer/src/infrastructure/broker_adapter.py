import json
import logging
import time
import pika
from src.config import settings
from src.domain.models import TelemetryAlert
from src.domain.ports import MessagePublisher

# Setup module logger
logger = logging.getLogger(__name__)

class RabbitMQPublisher(MessagePublisher):
    """
    RabbitMQ implementation of the MessagePublisher port.
    Includes reconnection logic and connection retries.
    """
    def __init__(self):
        self.host = settings.RABBITMQ_HOST
        self.port = settings.RABBITMQ_PORT
        self.username = settings.RABBITMQ_USER
        self.password = settings.RABBITMQ_PASSWORD
        self.queue_name = settings.RABBITMQ_QUEUE
        
        self.connection = None
        self.channel = None
        
        # Proactively attempt connection on initialization
        self._connect()

    def _connect(self) -> bool:
        """
        Attempts to establish a connection to RabbitMQ with retries.
        """
        if self.connection and not self.connection.is_closed:
            return True
            
        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        
        max_retries = 5
        retry_delay = 2  # Seconds
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempting to connect to RabbitMQ broker (Attempt {attempt}/{max_retries})...")
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Make queue durable so it persists across broker restarts
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                logger.info(f"Successfully connected to RabbitMQ and declared queue '{self.queue_name}'.")
                return True
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"RabbitMQ connection failed on attempt {attempt}: {str(e)}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("Failed to connect to RabbitMQ broker after maximum retries.")
                    return False

    def publish(self, alert: TelemetryAlert) -> bool:
        """
        Publishes the telemetry alert to the RabbitMQ queue as a JSON payload.
        """
        try:
            # Reconnect if connection was dropped
            if not self._connect():
                logger.error("Publish aborted: RabbitMQ broker is unreachable.")
                return False
                
            # Serialize the domain alert payload to JSON
            # datetime needs to be converted to ISO string
            payload = alert.model_dump()
            payload['timestamp'] = payload['timestamp'].isoformat()
            message_body = json.dumps(payload)
            
            # Publish as persistent message to ensure durability
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )
            logger.info(f"Published alert {alert.alert_id} successfully to queue '{self.queue_name}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to publish alert {alert.alert_id} to RabbitMQ: {str(e)}")
            return False

    def close(self) -> None:
        """
        Closes connection channels gracefully.
        """
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info("RabbitMQ connections closed gracefully.")
        except Exception as e:
            logger.error(f"Error during RabbitMQ connection close: {str(e)}")

import json
import logging
import time
from datetime import datetime
import pika
from typing import Callable
from src.config import settings
from src.domain.models import TelemetryAlert
from src.domain.ports import MessageConsumer

# Setup module logger
logger = logging.getLogger(__name__)

class RabbitMQConsumer(MessageConsumer):
    """
    RabbitMQ implementation of the MessageConsumer port.
    Manages active polling/listening on the queue with recovery retries.
    """
    def __init__(self):
        self.host = settings.RABBITMQ_HOST
        self.port = settings.RABBITMQ_PORT
        self.username = settings.RABBITMQ_USER
        self.password = settings.RABBITMQ_PASSWORD
        self.queue_name = settings.RABBITMQ_QUEUE
        
        self.connection = None
        self.channel = None
        self.is_running = False

    def _connect(self) -> bool:
        """
        Attempts to establish connection to RabbitMQ broker with exponential backoff.
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
                logger.info(f"Consumer attempting connection to RabbitMQ (Attempt {attempt}/{max_retries})...")
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Enforce queue durability
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                
                # Prefetch count = 1 to distribute load fairly
                self.channel.basic_qos(prefetch_count=1)
                logger.info(f"Consumer connected to RabbitMQ. Listening on queue '{self.queue_name}'.")
                return True
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"Consumer connection failure on attempt {attempt}: {str(e)}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logger.error("Consumer connection to RabbitMQ broker failed completely.")
                    return False

    def start_consuming(self, on_message_callback: Callable[[TelemetryAlert], None]) -> None:
        """
        Subscribes to RabbitMQ queue and starts consumption loop.
        This call is blocking and should be run in a separate thread.
        """
        self.is_running = True
        
        while self.is_running:
            try:
                if not self._connect():
                    logger.error("Cannot start consumption: RabbitMQ unreachable. Retrying in 10s...")
                    time.sleep(10)
                    continue
                
                def pika_callback(ch, method, properties, body):
                    try:
                        logger.info("Message received from queue. Processing payload...")
                        raw_data = json.loads(body.decode('utf-8'))
                        
                        # Parse strings back into appropriate object values
                        # convert ISO string timestamp back to datetime
                        alert_time = datetime.fromisoformat(raw_data['timestamp'])
                        
                        domain_alert = TelemetryAlert(
                            alert_id=raw_data['alert_id'],
                            patient_id=raw_data['patient_id'],
                            patient_name=raw_data['patient_name'],
                            heart_rate=raw_data['heart_rate'],
                            blood_pressure=raw_data['blood_pressure'],
                            alert_level=raw_data['alert_level'],
                            timestamp=alert_time,
                            received_at=datetime.utcnow()  # Log consumption moment
                        )
                        
                        # Trigger local port callback (saving to DB)
                        on_message_callback(domain_alert)
                        
                        # Acknowledge message delivery
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        logger.info(f"Message {domain_alert.alert_id} consumed and acknowledged successfully.")
                    except Exception as e:
                        logger.error(f"Failed to process message body: {str(e)}")
                        # Re-enqueue message in case of temporary DB failure
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                
                # Start subscribing
                self.channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=pika_callback
                )
                self.channel.start_consuming()
                
            except pika.exceptions.AMQPConnectionError:
                logger.warning("RabbitMQ connection lost. Retrying consumption loop...")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop: {str(e)}")
                time.sleep(5)

    def stop_consuming(self) -> None:
        """
        Stops consumption loop and cleans up connections.
        """
        logger.info("Stopping RabbitMQ consumer daemon...")
        self.is_running = False
        try:
            if self.channel and self.channel.is_open:
                # Need to run thread-safe method or stop consuming directly
                self.channel.stop_consuming()
            if self.connection and self.connection.is_open:
                self.connection.close()
            logger.info("RabbitMQ consumer connections stopped cleanly.")
        except Exception as e:
            logger.error(f"Failed to stop consumer cleanly: {str(e)}")
        finally:
            self.channel = None
            self.connection = None

import pika
import sys

# Connection configurations for the RabbitMQ server
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_VIRTUAL_HOST = "/uci_app"

# Exchange name constants representing physical domains
EXCHANGE_BIOSECURITY = "uci.bioseguridad"  # Fanout exchange for broadcast notifications
EXCHANGE_ALERTS = "uci.alertas"            # Direct exchange for severity-based alerts
EXCHANGE_MONITORING = "uci.monitoreo"      # Topic exchange for sensor telemetry data

# Queue name constants mapping to specific business components
QUEUE_MEDICAL_MONITOR = "cola.monitoreo_medico"
QUEUE_CRITICAL_ALERTS = "cola.alertas_criticas"
QUEUE_GENERAL_NOTICES = "cola.notificaciones_generales"

def get_rabbitmq_connection():
    """
    Establishes and returns a connection to RabbitMQ with custom virtual host parameters.
    Catches credentials and virtual host errors to guide the developer.
    """
    user_credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASSWORD)
    connection_parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VIRTUAL_HOST,
        credentials=user_credentials
    )
    
    try:
        connection_instance = pika.BlockingConnection(parameters=connection_parameters)
        return connection_instance
    except pika.exceptions.ProbableAuthenticationError:
        print("[ERROR] RabbitMQ Authentication failed. Please check user credentials.", file=sys.stderr)
        raise
    except pika.exceptions.ProbableAccessDeniedError:
        print(f"[ERROR] Access denied. Ensure that the virtual host '{RABBITMQ_VIRTUAL_HOST}' exists in RabbitMQ.", file=sys.stderr)
        print("[TIP] You can create it via the Management Web Interface (Admin -> Virtual Hosts) or by running:", file=sys.stderr)
        print(f"      rabbitmqctl add_vhost {RABBITMQ_VIRTUAL_HOST}", file=sys.stderr)
        print("      rabbitmqctl set_permissions -p /uci_app guest \".*\" \".*\" \".*\"", file=sys.stderr)
        raise
    except pika.exceptions.AMQPConnectionError:
        print("[ERROR] Could not connect to RabbitMQ broker. Is RabbitMQ running on localhost?", file=sys.stderr)
        raise

def setup_infrastructure(amqp_channel):
    """
    Declares all exchanges, queues, and binds them.
    Guarantees idempotency (creates them if they do not exist, does nothing otherwise).
    """
    # 1. Declare the exchanges with correct exchange types
    amqp_channel.exchange_declare(
        exchange=EXCHANGE_BIOSECURITY,
        exchange_type="fanout",
        durable=True
    )
    
    amqp_channel.exchange_declare(
        exchange=EXCHANGE_ALERTS,
        exchange_type="direct",
        durable=True
    )
    
    amqp_channel.exchange_declare(
        exchange=EXCHANGE_MONITORING,
        exchange_type="topic",
        durable=True
    )

    # 2. Declare durable queues for reliable messaging
    amqp_channel.queue_declare(queue=QUEUE_MEDICAL_MONITOR, durable=True)
    amqp_channel.queue_declare(queue=QUEUE_CRITICAL_ALERTS, durable=True)
    amqp_channel.queue_declare(queue=QUEUE_GENERAL_NOTICES, durable=True)

    # 3. Bind medical monitoring queue to topic exchange
    # Routes vitals: cama.<bed_number>.<sensor_type>
    vital_routing_keys = [
        "cama.*.ritmo_cardiaco",
        "cama.*.oxigeno",
        "cama.*.temperatura"
    ]
    for routing_pattern in vital_routing_keys:
        amqp_channel.queue_bind(
            queue=QUEUE_MEDICAL_MONITOR,
            exchange=EXCHANGE_MONITORING,
            routing_key=routing_pattern
        )

    # Bind medical queue to direct exchange for warning/critical clinical alerts
    clinical_alert_levels = ["warning", "critical"]
    for alert_level in clinical_alert_levels:
        amqp_channel.queue_bind(
            queue=QUEUE_MEDICAL_MONITOR,
            exchange=EXCHANGE_ALERTS,
            routing_key=alert_level
        )

    # 4. Bind critical security alerts queue to direct exchange
    # Receives only 'critical' severity infrastructure/security events
    amqp_channel.queue_bind(
        queue=QUEUE_CRITICAL_ALERTS,
        exchange=EXCHANGE_ALERTS,
        routing_key="critical"
    )

    # 5. Bind general notifications queue to biosecurity fanout exchange
    # Broadcasts to all connected receivers
    amqp_channel.queue_bind(
        queue=QUEUE_GENERAL_NOTICES,
        exchange=EXCHANGE_BIOSECURITY,
        routing_key=""
    )

    print("[SUCCESS] RabbitMQ exchange, queue, and binding topology verified successfully.")

import pika
import sys

# Configuraciones de conexión para el servidor RabbitMQ
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_USER = "guest"
RABBITMQ_PASSWORD = "guest"
RABBITMQ_VIRTUAL_HOST = "/uci_app"

# Constantes de nombres de Exchange que representan dominios físicos
EXCHANGE_BIOSECURITY = "uci.bioseguridad"  # Exchange Fanout para notificaciones de difusión general
EXCHANGE_ALERTS = "uci.alertas"            # Exchange Direct para alertas basadas en la gravedad
EXCHANGE_MONITORING = "uci.monitoreo"      # Exchange Topic para datos de telemetría de sensores

# Constantes de nombres de colas que se asignan a componentes de negocio específicos
QUEUE_MEDICAL_MONITOR = "cola.monitoreo_medico"
QUEUE_CRITICAL_ALERTS = "cola.alertas_criticas"
QUEUE_GENERAL_NOTICES = "cola.notificaciones_generales"

def get_rabbitmq_connection():
    """
    Establece y devuelve una conexión a RabbitMQ con parámetros de virtual host personalizados.
    Captura errores de credenciales y de virtual host para guiar al desarrollador.
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
        print("[ERROR] Falló la autenticación en RabbitMQ. Por favor verifique las credenciales de usuario.", file=sys.stderr)
        raise
    except pika.exceptions.ProbableAccessDeniedError:
        print(f"[ERROR] Acceso denegado. Asegúrese de que el virtual host '{RABBITMQ_VIRTUAL_HOST}' exista en RabbitMQ.", file=sys.stderr)
        print("[CONSEJO] Puede crearlo desde la interfaz web de administración (Admin -> Virtual Hosts) o ejecutando:", file=sys.stderr)
        print(f"          rabbitmqctl add_vhost {RABBITMQ_VIRTUAL_HOST}", file=sys.stderr)
        print("          rabbitmqctl set_permissions -p /uci_app guest \".*\" \".*\" \".*\"", file=sys.stderr)
        raise
    except pika.exceptions.AMQPConnectionError:
        print("[ERROR] No se pudo conectar al servidor RabbitMQ. ¿Está RabbitMQ ejecutándose en localhost?", file=sys.stderr)
        raise

def setup_infrastructure(amqp_channel):
    """
    Declara todos los exchanges, colas y realiza sus enlaces (bindings).
    Garantiza la idempotencia (los crea si no existen, de lo contrario no hace nada).
    """
    # 1. Declarar los exchanges con los tipos de exchange correctos
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

    # 2. Declarar colas duraderas para una mensajería confiable
    amqp_channel.queue_declare(queue=QUEUE_MEDICAL_MONITOR, durable=True)
    amqp_channel.queue_declare(queue=QUEUE_CRITICAL_ALERTS, durable=True)
    amqp_channel.queue_declare(queue=QUEUE_GENERAL_NOTICES, durable=True)

    # 3. Enlazar la cola de monitoreo médico al exchange topic
    # Enruta signos vitales: cama.<numero_cama>.<tipo_sensor>
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

    # Enlazar la cola médica al exchange direct para alertas clínicas de advertencia (warning) y críticas (critical)
    clinical_alert_levels = ["warning", "critical"]
    for alert_level in clinical_alert_levels:
        amqp_channel.queue_bind(
            queue=QUEUE_MEDICAL_MONITOR,
            exchange=EXCHANGE_ALERTS,
            routing_key=alert_level
        )

    # 4. Enlazar la cola de alertas críticas de seguridad al exchange direct
    # Recibe solo eventos de seguridad/infraestructura de severidad 'critical'
    amqp_channel.queue_bind(
        queue=QUEUE_CRITICAL_ALERTS,
        exchange=EXCHANGE_ALERTS,
        routing_key="critical"
    )

    # 5. Enlazar la cola de notificaciones generales al exchange fanout de bioseguridad
    # Realiza una difusión (broadcast) a todos los receptores conectados
    amqp_channel.queue_bind(
        queue=QUEUE_GENERAL_NOTICES,
        exchange=EXCHANGE_BIOSECURITY,
        routing_key=""
    )

    print("[ÉXITO] La topología de exchanges, colas y enlaces de RabbitMQ se verificó correctamente.")

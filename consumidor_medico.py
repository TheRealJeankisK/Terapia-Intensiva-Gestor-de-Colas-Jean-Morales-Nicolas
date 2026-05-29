import json
import time
import sys
import pika

import config

def format_terminal_output(routing_key, payload):
    """
    Muestra los datos de telemetría con colores personalizados basados en los niveles de gravedad.
    """
    severity = payload.get("severity_level", "info").upper()
    bed = payload.get("bed_number", "UNKNOWN")
    sensor = payload.get("sensor_type", "UNKNOWN")
    value = payload.get("metric_value", 0.0)
    unit = payload.get("measurement_unit", "")
    description = payload.get("description", "")
    
    # Códigos de escape ANSI para colorear la salida de consola
    color_reset = "\033[0m"
    color_bold = "\033[1m"
    color_red = "\033[91m"
    color_yellow = "\033[93m"
    color_cyan = "\033[96m"
    color_green = "\033[92m"
    
    # Selecciona el color según la gravedad
    if severity == "CRITICAL" or severity == "CRÍTICA" or severity == "CRITICA":
        alert_prefix = f"{color_red}{color_bold}[ALERTA CLÍNICA CRÍTICA]{color_reset}"
        severity_formatted = f"{color_red}{severity}{color_reset}"
    elif severity == "WARNING" or severity == "ADVERTENCIA":
        alert_prefix = f"{color_yellow}{color_bold}[EVENTO CLÍNICO DE ADVERTENCIA]{color_reset}"
        severity_formatted = f"{color_yellow}{severity}{color_reset}"
    else:
        alert_prefix = f"{color_green}[TELEMETRÍA CLÍNICA INFORMATIVA]{color_reset}"
        severity_formatted = f"{color_cyan}{severity}{color_reset}"
        
    print(f"\n{alert_prefix}")
    print(f"  [Clave de Ruteo] : {routing_key}")
    print(f"  [Cama Paciente]  : Cama #{bed}")
    print(f"  [Medición]       : {sensor} = {value} {unit}")
    print(f"  [Severidad]      : {severity_formatted}")
    print(f"  [Descripción]    : {description}")
    print("-" * 50)

def medical_message_callback(amqp_channel, delivery_method, message_properties, raw_message_body):
    """
    Callback de Pika que se activa cuando se recibe un mensaje de la cola.
    Procesa el payload JSON y confirma manualmente la recepción tras una ejecución exitosa.
    """
    try:
        # Decodifica y deserializa el mensaje JSON
        payload_dictionary = json.loads(raw_message_body.decode('utf-8'))
        
        # Muestra los detalles de enrutamiento del mensaje según lo solicitado
        format_terminal_output(delivery_method.routing_key, payload_dictionary)
        
        # Simula el almacenamiento en la base de datos local o el retraso de análisis
        time.sleep(1.0)
        
        # Confirma manualmente la finalización del procesamiento del mensaje
        amqp_channel.basic_ack(delivery_tag=delivery_method.delivery_tag)
        print("[ACK] Mensaje procesado y confirmado (Acknowledge) con éxito.")
        
    except json.JSONDecodeError as decode_exception:
        print(f"[ERROR] Error al decodificar el cuerpo JSON: {decode_exception}", file=sys.stderr)
        # Rechaza formatos JSON inválidos sin reencolar para prevenir bucles infinitos por mensajes venenosos
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=False)
        
    except Exception as processing_exception:
        print(f"[ERROR] Ocurrió una excepción al procesar el evento médico: {processing_exception}", file=sys.stderr)
        # Reencola el mensaje para intentar procesarlo más tarde
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=True)
        print("[NACK] Falló el procesamiento del mensaje. Reencolado para reintento.")

def main():
    """
    Inicializa la conexión del consumidor médico e inicia el consumo bloqueante de la cola.
    """
    print("=" * 60)
    print("  CONSUMIDOR: Monitor Clínico de UCI (Telemetría Médica)  ")
    print("=" * 60)
    
    try:
        rabbitmq_connection = config.get_rabbitmq_connection()
        amqp_channel = rabbitmq_connection.channel()
        
        # Asegura que la infraestructura esté configurada (configuración idempotente de la topología)
        config.setup_infrastructure(amqp_channel)
        
        # Despacho equitativo: Prefetch 1 mensaje para equilibrar la carga entre posibles instancias de escala
        amqp_channel.basic_qos(prefetch_count=1)
        
        # Configura el consumidor para consumir con confirmación manual (basic_ack)
        amqp_channel.basic_consume(
            queue=config.QUEUE_MEDICAL_MONITOR,
            on_message_callback=medical_message_callback,
            auto_ack=False
        )
        
        print(f"\n[*] Escuchando en la cola: '{config.QUEUE_MEDICAL_MONITOR}'")
        print("[*] Presione Ctrl+C para finalizar el consumidor de forma segura.\n")
        
        amqp_channel.start_consuming()
        
    except KeyboardInterrupt:
        print("\n[PARADA] Consumidor Médico detenido por el usuario.")
    except Exception as initialization_exception:
        print(f"[FATAL] Error de inicialización del consumidor: {initialization_exception}", file=sys.stderr)
    finally:
        # Cierra las conexiones de forma limpia
        try:
            if 'rabbitmq_connection' in locals() and rabbitmq_connection.is_open:
                rabbitmq_connection.close()
                print("[INFO] Conexión a RabbitMQ cerrada limpiamente.")
        except Exception:
            pass

if __name__ == "__main__":
    main()

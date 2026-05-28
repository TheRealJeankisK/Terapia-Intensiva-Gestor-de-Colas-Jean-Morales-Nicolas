import json
import time
import sys
import pika

import config

def format_security_output(exchange_name, routing_key, payload):
    """
    Renders high-visibility security alerts on the terminal based on the originating exchange.
    """
    severity = payload.get("severity_level", "info").upper()
    bed = payload.get("bed_number", "SISTEMA")
    sensor = payload.get("sensor_type", "SEGURIDAD")
    description = payload.get("description", "")
    
    color_reset = "\033[0m"
    color_bold = "\033[1m"
    color_red_background = "\033[41m"
    color_yellow_background = "\033[43m"
    color_white = "\033[97m"
    color_red = "\033[91m"
    color_yellow = "\033[93m"
    
    # Check if message came from the Biosecurity Fanout exchange
    if exchange_name == config.EXCHANGE_BIOSECURITY:
        print("\n" + "#" * 60)
        print(f"{color_yellow_background}{color_white}{color_bold} [COMUNICADO DE BIOSEGURIDAD DIFUNDIDO] {color_reset}")
        print(f"  [Exchange de Origen] : {exchange_name}")
        print(f"  [Área Afectada]      : {bed}")
        print(f"  [Mensaje de Evento]  : {description}")
        print(f"  [Acción Requerida]   : Por favor, verifique los registros de bioseguridad del hospital.")
        print("#" * 60)
        
    # Check if message is a Direct Critical Alert
    elif exchange_name == config.EXCHANGE_ALERTS and routing_key == "critical":
        print("\n" + "!" * 60)
        print(f"{color_red_background}{color_white}{color_bold} !!! EMERGENCIA FÍSICA / INFRAESTRUCTURA CRÍTICA !!! {color_reset}")
        print(f"  [Exchange de Origen] : {exchange_name}")
        print(f"  [Clave de Ruteo]     : {routing_key}")
        print(f"  [Dispositivo Origen] : {sensor} (Ubicación: {bed})")
        print(f"  [Severidad]          : {color_red}{color_bold}{severity}{color_reset}")
        print(f"  [Detalles]           : {description}")
        print(f"  [Acción Requerida]   : ACTIVANDO SIRENAS Y PROTOCOLO DE EMERGENCIA B-12")
        print("!" * 60)
        
    else:
        # Fallback format for any other unexpected messages routing to the security queues
        print(f"\n[REPORTE DE SEGURIDAD] Exchange: {exchange_name} | Clave: {routing_key}")
        print(f"  Detalles: {description}")
        print("-" * 50)

def security_message_callback(amqp_channel, delivery_method, message_properties, raw_message_body):
    """
    Pika callback triggered when a message arrives on either of the security queues.
    Deserializes the JSON event, displays appropriate sirens, and acknowledges the message.
    """
    try:
        # Decode and deserialize the JSON payload
        payload_dictionary = json.loads(raw_message_body.decode('utf-8'))
        
        # Display formatted output
        format_security_output(
            exchange_name=delivery_method.exchange,
            routing_key=delivery_method.routing_key,
            payload=payload_dictionary
        )
        
        # Simulate alarm activation/logging system latency
        time.sleep(1.0)
        
        # Manually acknowledge the message
        amqp_channel.basic_ack(delivery_tag=delivery_method.delivery_tag)
        print("[ACK] Mensaje de seguridad procesado y confirmado (Acknowledged).")
        
    except json.JSONDecodeError as decode_exception:
        print(f"[ERROR] Error al decodificar el cuerpo JSON: {decode_exception}", file=sys.stderr)
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=False)
        
    except Exception as processing_exception:
        print(f"[ERROR] Ocurrió una excepción al procesar el evento de seguridad: {processing_exception}", file=sys.stderr)
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=True)
        print("[NACK] Falló el procesamiento del mensaje. Reencolado para reintento.")

def main():
    """
    Initializes connection and consumes from multiple security queues concurrently.
    """
    print("=" * 60)
    print("  CONSUMIDOR: Panel de Operaciones de Seguridad e Infraestructura  ")
    print("=" * 60)
    
    try:
        rabbitmq_connection = config.get_rabbitmq_connection()
        amqp_channel = rabbitmq_connection.channel()
        
        # Ensure infrastructure is set up (Idempotent topology setup)
        config.setup_infrastructure(amqp_channel)
        
        # Fair dispatch: Prefetch 1 message
        amqp_channel.basic_qos(prefetch_count=1)
        
        # Consume from general biosecurity notifications queue (Fanout)
        amqp_channel.basic_consume(
            queue=config.QUEUE_GENERAL_NOTICES,
            on_message_callback=security_message_callback,
            auto_ack=False
        )
        
        # Consume from critical safety alerts queue (Direct - critical)
        amqp_channel.basic_consume(
            queue=config.QUEUE_CRITICAL_ALERTS,
            on_message_callback=security_message_callback,
            auto_ack=False
        )
        
        print(f"\n[*] Escuchando en las colas: '{config.QUEUE_GENERAL_NOTICES}' y '{config.QUEUE_CRITICAL_ALERTS}'")
        print("[*] Presione Ctrl+C para finalizar el consumidor de forma segura.\n")
        
        amqp_channel.start_consuming()
        
    except KeyboardInterrupt:
        print("\n[PARADA] Consumidor de Seguridad detenido por el usuario.")
    except Exception as initialization_exception:
        print(f"[FATAL] Error de inicialización del consumidor: {initialization_exception}", file=sys.stderr)
    finally:
        # Gracefully close connections
        try:
            if 'rabbitmq_connection' in locals() and rabbitmq_connection.is_open:
                rabbitmq_connection.close()
                print("[INFO] Conexión a RabbitMQ cerrada limpiamente.")
        except Exception:
            pass

if __name__ == "__main__":
    main()

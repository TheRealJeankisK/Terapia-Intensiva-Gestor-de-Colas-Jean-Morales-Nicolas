import json
import time
import sys
import pika

import config

def format_security_output(exchange_name, routing_key, payload):
    """
    Muestra alertas de seguridad de alta visibilidad en la terminal basadas en el exchange de origen.
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
    
    # Verifica si el mensaje provino del exchange Fanout de Bioseguridad
    if exchange_name == config.EXCHANGE_BIOSECURITY:
        print("\n" + "#" * 60)
        print(f"{color_yellow_background}{color_white}{color_bold} [COMUNICADO DE BIOSEGURIDAD DIFUNDIDO] {color_reset}")
        print(f"  [Exchange de Origen] : {exchange_name}")
        print(f"  [Área Afectada]      : {bed}")
        print(f"  [Mensaje de Evento]  : {description}")
        print(f"  [Acción Requerida]   : Por favor, verifique los registros de bioseguridad del hospital.")
        print("#" * 60)
        
    # Verifica si el mensaje es una Alerta Crítica Directa
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
        # Formato de respaldo para cualquier otro mensaje inesperado enrutado a las colas de seguridad
        print(f"\n[REPORTE DE SEGURIDAD] Exchange: {exchange_name} | Clave: {routing_key}")
        print(f"  Detalles: {description}")
        print("-" * 50)

def security_message_callback(amqp_channel, delivery_method, message_properties, raw_message_body):
    """
    Callback de Pika que se activa cuando llega un mensaje a cualquiera de las colas de seguridad.
    Deserializa el evento JSON, muestra las alarmas apropiadas y confirma el mensaje.
    """
    try:
        # Decodifica y deserializa el payload JSON
        payload_dictionary = json.loads(raw_message_body.decode('utf-8'))
        
        # Muestra la salida formateada
        format_security_output(
            exchange_name=delivery_method.exchange,
            routing_key=delivery_method.routing_key,
            payload=payload_dictionary
        )
        
        # Simula la activación de la alarma/latencia del sistema de registro
        time.sleep(1.0)
        
        # Confirma manualmente el mensaje
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
    Inicializa la conexión y consume de múltiples colas de seguridad de forma concurrente.
    """
    print("=" * 60)
    print("  CONSUMIDOR: Panel de Operaciones de Seguridad e Infraestructura  ")
    print("=" * 60)
    
    try:
        rabbitmq_connection = config.get_rabbitmq_connection()
        amqp_channel = rabbitmq_connection.channel()
        
        # Asegura que la infraestructura esté configurada (configuración idempotente de la topología)
        config.setup_infrastructure(amqp_channel)
        
        # Despacho equitativo: Prefetch de 1 mensaje
        amqp_channel.basic_qos(prefetch_count=1)
        
        # Consume de la cola de notificaciones generales de bioseguridad (Fanout)
        amqp_channel.basic_consume(
            queue=config.QUEUE_GENERAL_NOTICES,
            on_message_callback=security_message_callback,
            auto_ack=False
        )
        
        # Consume de la cola de alertas de seguridad críticas (Direct - critical)
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
        # Cierra las conexiones de forma limpia
        try:
            if 'rabbitmq_connection' in locals() and rabbitmq_connection.is_open:
                rabbitmq_connection.close()
                print("[INFO] Conexión a RabbitMQ cerrada limpiamente.")
        except Exception:
            pass

if __name__ == "__main__":
    main()

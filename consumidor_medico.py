import json
import time
import sys
import pika

import config

def format_terminal_output(routing_key, payload):
    """
    Renders telemetry data with customized colors based on severity levels.
    """
    severity = payload.get("severity_level", "info").upper()
    bed = payload.get("bed_number", "UNKNOWN")
    sensor = payload.get("sensor_type", "UNKNOWN")
    value = payload.get("metric_value", 0.0)
    unit = payload.get("measurement_unit", "")
    description = payload.get("description", "")
    
    # ANSI escape codes for coloring console output
    color_reset = "\033[0m"
    color_bold = "\033[1m"
    color_red = "\033[91m"
    color_yellow = "\033[93m"
    color_cyan = "\033[96m"
    color_green = "\033[92m"
    
    # Choose color based on severity
    if severity == "CRITICAL":
        alert_prefix = f"{color_red}{color_bold}[CRITICAL CLINICAL ALERT]{color_reset}"
        severity_formatted = f"{color_red}{severity}{color_reset}"
    elif severity == "WARNING":
        alert_prefix = f"{color_yellow}{color_bold}[WARNING CLINICAL EVENT]{color_reset}"
        severity_formatted = f"{color_yellow}{severity}{color_reset}"
    else:
        alert_prefix = f"{color_green}[INFO CLINICAL TELEMETRY]{color_reset}"
        severity_formatted = f"{color_cyan}{severity}{color_reset}"
        
    print(f"\n{alert_prefix}")
    print(f"  [Routing Key]   : {routing_key}")
    print(f"  [Patient Bed]   : Bed #{bed}")
    print(f"  [Measurement]   : {sensor} = {value} {unit}")
    print(f"  [Severity]      : {severity_formatted}")
    print(f"  [Description]   : {description}")
    print("-" * 50)

def medical_message_callback(amqp_channel, delivery_method, message_properties, raw_message_body):
    """
    Pika callback triggered when a message is received from the queue.
    Processes the JSON payload and manually acknowledges successful execution.
    """
    try:
        # Decode and deserialize the JSON message
        payload_dictionary = json.loads(raw_message_body.decode('utf-8'))
        
        # Display message routing details as requested
        format_terminal_output(delivery_method.routing_key, payload_dictionary)
        
        # Simulate local database storage or analytics delay
        time.sleep(1.0)
        
        # Manually acknowledge the message processing completion
        amqp_channel.basic_ack(delivery_tag=delivery_method.delivery_tag)
        print("[ACK] Message processed and acknowledged successfully.")
        
    except json.JSONDecodeError as decode_exception:
        print(f"[ERROR] Failed to decode JSON payload: {decode_exception}", file=sys.stderr)
        # Reject invalid JSON formats without requeuing to prevent poison queue loops
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=False)
        
    except Exception as processing_exception:
        print(f"[ERROR] Exception occurred while processing medical event: {processing_exception}", file=sys.stderr)
        # Requeue message to attempt reprocessing later
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=True)
        print("[NACK] Message processing failed. Requeued for retry.")

def main():
    """
    Initializes medical consumer connection and starts blocking queue consumption.
    """
    print("=" * 60)
    print("    UCI Clinical Monitor Consumer (Medical Telemetry)    ")
    print("=" * 60)
    
    try:
        rabbitmq_connection = config.get_rabbitmq_connection()
        amqp_channel = rabbitmq_connection.channel()
        
        # Ensure infrastructure is set up (Idempotent topology setup)
        config.setup_infrastructure(amqp_channel)
        
        # Fair dispatch: Prefetch 1 message to balance load among potential scaling instances
        amqp_channel.basic_qos(prefetch_count=1)
        
        # Configure consumer to consume with manual basic_ack
        amqp_channel.basic_consume(
            queue=config.QUEUE_MEDICAL_MONITOR,
            on_message_callback=medical_message_callback,
            auto_ack=False
        )
        
        print(f"\n[*] Listening on queue: '{config.QUEUE_MEDICAL_MONITOR}'")
        print("[*] Press Ctrl+C to terminate consumer safely.\n")
        
        amqp_channel.start_consuming()
        
    except KeyboardInterrupt:
        print("\n[STOP] Medical Consumer terminated by user.")
    except Exception as initialization_exception:
        print(f"[FATAL] Consumer initialization error: {initialization_exception}", file=sys.stderr)
    finally:
        # Gracefully close connections
        try:
            if 'rabbitmq_connection' in locals() and rabbitmq_connection.is_open:
                rabbitmq_connection.close()
                print("[INFO] RabbitMQ connection closed cleanly.")
        except Exception:
            pass

if __name__ == "__main__":
    main()

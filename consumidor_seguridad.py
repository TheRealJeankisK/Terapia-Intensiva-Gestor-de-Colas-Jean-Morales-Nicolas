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
    bed = payload.get("bed_number", "SYSTEM")
    sensor = payload.get("sensor_type", "SECURITY")
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
        print(f"{color_yellow_background}{color_white}{color_bold} [BIOSECURITY BROADCAST NOTICE] {color_reset}")
        print(f"  [Origin Exchange] : {exchange_name}")
        print(f"  [Affected Area]   : {bed}")
        print(f"  [Event Message]   : {description}")
        print(f"  [Action Required] : Please verify hospital biosecurity logs.")
        print("#" * 60)
        
    # Check if message is a Direct Critical Alert
    elif exchange_name == config.EXCHANGE_ALERTS and routing_key == "critical":
        print("\n" + "!" * 60)
        print(f"{color_red_background}{color_white}{color_bold} !!! CRITICAL INFRASTRUCTURE / PHYSICAL EMERGENCY !!! {color_reset}")
        print(f"  [Origin Exchange] : {exchange_name}")
        print(f"  [Routing Key]     : {routing_key}")
        print(f"  [Trigger Device]  : {sensor} (Location: {bed})")
        print(f"  [Severity]        : {color_red}{color_bold}{severity}{color_reset}")
        print(f"  [Details]         : {description}")
        print(f"  [Action Required] : ACTIVATING SIRENS & EMERGENCY PROTOCOL B-12")
        print("!" * 60)
        
    else:
        # Fallback format for any other unexpected messages routing to the security queues
        print(f"\n[SECURITY REPORT] Exchange: {exchange_name} | Key: {routing_key}")
        print(f"  Details: {description}")
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
        print("[ACK] Security message processed and acknowledged.")
        
    except json.JSONDecodeError as decode_exception:
        print(f"[ERROR] Failed to decode JSON payload: {decode_exception}", file=sys.stderr)
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=False)
        
    except Exception as processing_exception:
        print(f"[ERROR] Exception occurred while processing security event: {processing_exception}", file=sys.stderr)
        amqp_channel.basic_nack(delivery_tag=delivery_method.delivery_tag, requeue=True)
        print("[NACK] Security message processing failed. Requeued for retry.")

def main():
    """
    Initializes connection and consumes from multiple security queues concurrently.
    """
    print("=" * 60)
    print("    UCI Security & Infrastructure Operations Dashboard    ")
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
        
        print(f"\n[*] Listening on queues: '{config.QUEUE_GENERAL_NOTICES}' & '{config.QUEUE_CRITICAL_ALERTS}'")
        print("[*] Press Ctrl+C to terminate consumer safely.\n")
        
        amqp_channel.start_consuming()
        
    except KeyboardInterrupt:
        print("\n[STOP] Security Consumer terminated by user.")
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

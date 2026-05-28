import json
import time
import uuid
import random
import sys
from datetime import datetime
import pika

import config

def generate_medical_payload(bed_number, sensor_type, metric_value, unit, severity_level, description):
    """
    Constructs a structured JSON payload representing medical telemetry or clinical alerts.
    """
    message_uuid = str(uuid.uuid4())
    iso_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload_dictionary = {
        "message_id": message_uuid,
        "timestamp": iso_timestamp,
        "bed_number": bed_number,
        "sensor_type": sensor_type,
        "metric_value": metric_value,
        "measurement_unit": unit,
        "severity_level": severity_level,
        "description": description
    }
    return payload_dictionary

def publish_message(amqp_channel, exchange_name, routing_key, payload_dictionary):
    """
    Serializes payload to JSON and publishes it to the specified exchange.
    """
    serialized_payload = json.dumps(payload_dictionary, indent=2)
    
    # Message properties indicating JSON content type and persistent delivery mode (2)
    message_properties = pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2  
    )
    
    amqp_channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key,
        body=serialized_payload,
        properties=message_properties
    )
    
    print("-" * 50)
    print(f"[PUBLISHED] Exchange: '{exchange_name}' | Routing Key: '{routing_key}'")
    print(f"Payload: {json.dumps(payload_dictionary)}")
    print("-" * 50)

def handle_manual_telemetry(amqp_channel):
    """
    Prompts the user for telemetry details and publishes it to the Topic exchange.
    """
    print("\n--- Publish Vital Signs Telemetry (Topic Exchange) ---")
    bed_input = input("Enter bed number (e.g. 05, 12) [Default: 08]: ").strip() or "08"
    
    print("Select sensor type:")
    print("1. Ritmo Cardíaco (heart rate)")
    print("2. Oxígeno (oxygen saturation)")
    print("3. Temperatura (temperature)")
    sensor_choice = input("Choice (1-3): ").strip()
    
    if sensor_choice == "1":
        sensor_type = "ritmo_cardiaco"
        metric_value = float(input("Enter heart rate (bpm) [Default: 72]: ").strip() or "72")
        unit = "bpm"
        # Evaluate severity
        if metric_value < 50 or metric_value > 120:
            severity_level = "critical"
            description = "Abnormal heart rate detected!"
        elif metric_value < 60 or metric_value > 100:
            severity_level = "warning"
            description = "Heart rate slightly out of range."
        else:
            severity_level = "info"
            description = "Heart rate normal."
            
    elif sensor_choice == "2":
        sensor_type = "oxigeno"
        metric_value = float(input("Enter oxygen SpO2 (%) [Default: 97]: ").strip() or "97")
        unit = "%"
        if metric_value < 90:
            severity_level = "critical"
            description = "Hypoxia warning! Critical oxygen saturation levels."
        elif metric_value < 94:
            severity_level = "warning"
            description = "Low oxygen levels."
        else:
            severity_level = "info"
            description = "Oxygen levels normal."
            
    else:
        sensor_type = "temperatura"
        metric_value = float(input("Enter temperature (°C) [Default: 36.8]: ").strip() or "36.8")
        unit = "C"
        if metric_value > 38.5 or metric_value < 35.0:
            severity_level = "critical"
            description = "Critical temperature spike or hypothermia!"
        elif metric_value > 37.5 or metric_value < 36.0:
            severity_level = "warning"
            description = "Fever warning."
        else:
            severity_level = "info"
            description = "Temperature normal."

    # Routing key pattern: cama.<bed_number>.<sensor_type>
    routing_key_topic = f"cama.{bed_input}.{sensor_type}"
    payload_dictionary = generate_medical_payload(
        bed_number=bed_input,
        sensor_type=sensor_type,
        metric_value=metric_value,
        unit=unit,
        severity_level=severity_level,
        description=description
    )
    
    publish_message(amqp_channel, config.EXCHANGE_MONITORING, routing_key_topic, payload_dictionary)

def handle_manual_alert(amqp_channel):
    """
    Prompts the user for alert severity and description and publishes to Direct exchange.
    """
    print("\n--- Publish Severity Alert (Direct Exchange) ---")
    print("Select severity level:")
    print("1. INFO (General information)")
    print("2. WARNING (Requires attention)")
    print("3. CRITICAL (Requires immediate response)")
    severity_choice = input("Choice (1-3): ").strip()
    
    severity_level = "info"
    if severity_choice == "2":
        severity_level = "warning"
    elif severity_choice == "3":
        severity_level = "critical"
        
    description = input("Enter alert message: ").strip() or "Standard diagnostic alert"
    
    payload_dictionary = generate_medical_payload(
        bed_number="SYSTEM",
        sensor_type="alerta_directa",
        metric_value=0.0,
        unit="N/A",
        severity_level=severity_level,
        description=description
    )
    
    publish_message(amqp_channel, config.EXCHANGE_ALERTS, severity_level, payload_dictionary)

def handle_manual_biosecurity(amqp_channel):
    """
    Prompts the user for a biosecurity message and broadcasts to Fanout exchange.
    """
    print("\n--- Publish Biosecurity Notice (Fanout Exchange) ---")
    description = input("Enter biosecurity message: ").strip() or "Standard biosecurity check required."
    
    payload_dictionary = generate_medical_payload(
        bed_number="ALL",
        sensor_type="bioseguridad",
        metric_value=0.0,
        unit="N/A",
        severity_level="warning",
        description=description
    )
    
    # Fanout routing key is ignored, empty string is used
    publish_message(amqp_channel, config.EXCHANGE_BIOSECURITY, "", payload_dictionary)

def run_automated_simulation(amqp_channel):
    """
    Runs an infinite loop simulating realistic patient readings and occasional critical events.
    """
    print("\n[START] Starting automated UCI patient telemetries simulation...")
    print("Press Ctrl+C to stop simulation and return to menu.\n")
    
    active_sensors = [
        {"type": "ritmo_cardiaco", "unit": "bpm", "normal_range": (60, 95), "critical_range": (40, 140)},
        {"type": "oxigeno", "unit": "%", "normal_range": (95, 100), "critical_range": (80, 89)},
        {"type": "temperatura", "unit": "C", "normal_range": (36.2, 37.2), "critical_range": (34.0, 40.0)}
    ]
    
    bed_identifiers = ["01", "02", "03", "04", "05"]
    
    try:
        while True:
            # 80% chance of publishing normal telemetry, 15% clinical alert, 5% biosecurity alert
            event_probability = random.random()
            
            if event_probability < 0.70:
                # Telemetry
                selected_bed = random.choice(bed_identifiers)
                selected_sensor = random.choice(active_sensors)
                
                # Determine if we simulate an abnormality (10% chance of abnormal reading)
                is_abnormal = random.random() < 0.10
                if is_abnormal:
                    metric_value = round(random.uniform(*selected_sensor["critical_range"]), 1)
                    severity_level = "critical"
                    description = f"Critical threshold breach for {selected_sensor['type']}!"
                else:
                    metric_value = round(random.uniform(*selected_sensor["normal_range"]), 1)
                    severity_level = "info"
                    description = f"Telemetry read normal for {selected_sensor['type']}."
                
                routing_key_topic = f"cama.{selected_bed}.{selected_sensor['type']}"
                payload_dictionary = generate_medical_payload(
                    bed_number=selected_bed,
                    sensor_type=selected_sensor["type"],
                    metric_value=metric_value,
                    unit=selected_sensor["unit"],
                    severity_level=severity_level,
                    description=description
                )
                
                publish_message(amqp_channel, config.EXCHANGE_MONITORING, routing_key_topic, payload_dictionary)
                
            elif event_probability < 0.90:
                # Direct Alert
                severity_level = random.choice(["info", "warning", "critical"])
                selected_bed = random.choice(bed_identifiers)
                
                descriptions = {
                    "info": f"Routine shift change update for bed {selected_bed}.",
                    "warning": f"Infusion pump warning: low volume remaining on bed {selected_bed}.",
                    "critical": f"ECG electrode disconnected on bed {selected_bed}!"
                }
                
                payload_dictionary = generate_medical_payload(
                    bed_number=selected_bed,
                    sensor_type="alerta_directa",
                    metric_value=0.0,
                    unit="N/A",
                    severity_level=severity_level,
                    description=descriptions[severity_level]
                )
                
                publish_message(amqp_channel, config.EXCHANGE_ALERTS, severity_level, payload_dictionary)
                
            else:
                # Fanout Biosecurity Notice
                hospital_notices = [
                    "Main power failure in North Wing - backup generator active.",
                    "Hazmat decontamination protocols activated in Operating Room 2.",
                    "Water supply maintenance scheduled for ICU sector at 22:00."
                ]
                
                payload_dictionary = generate_medical_payload(
                    bed_number="ALL",
                    sensor_type="bioseguridad",
                    metric_value=0.0,
                    unit="N/A",
                    severity_level="critical",
                    description=random.choice(hospital_notices)
                )
                
                publish_message(amqp_channel, config.EXCHANGE_BIOSECURITY, "", payload_dictionary)
            
            # Sleep 3 seconds between simulated events
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("\n[STOP] Automated simulation halted by user.")

def main():
    """
    Main entry point for the producer simulation cli.
    """
    print("=" * 60)
    print("  UCI Patient Telemetry & Security Alert Producer Simulator  ")
    print("=" * 60)
    
    # Establish connection and set up the channel
    try:
        rabbitmq_connection = config.get_rabbitmq_connection()
        amqp_channel = rabbitmq_connection.channel()
        
        # Ensure infrastructure exists before starting operations (Idempotence)
        config.setup_infrastructure(amqp_channel)
    except Exception as connection_exception:
        print(f"[FATAL] Connection initialization aborted. {connection_exception}")
        sys.exit(1)
        
    try:
        while True:
            print("\nSelect an action:")
            print("1. Publish manual Vital Signs Telemetry (Topic -> uci.monitoreo)")
            print("2. Publish manual Severity Alert (Direct -> uci.alertas)")
            print("3. Publish manual Biosecurity Broadcast (Fanout -> uci.bioseguridad)")
            print("4. Run automated real-time ICU simulation loop")
            print("5. Exit")
            
            main_menu_choice = input("Choice (1-5): ").strip()
            
            if main_menu_choice == "1":
                handle_manual_telemetry(amqp_channel)
            elif main_menu_choice == "2":
                handle_manual_alert(amqp_channel)
            elif main_menu_choice == "3":
                handle_manual_biosecurity(amqp_channel)
            elif main_menu_choice == "4":
                run_automated_simulation(amqp_channel)
            elif main_menu_choice == "5":
                print("Exiting producer simulator. Goodbye!")
                break
            else:
                print("[WARNING] Invalid choice. Please choose 1 to 5.")
                
    except KeyboardInterrupt:
        print("\nExiting producer. Closing connection.")
    finally:
        if rabbitmq_connection.is_open:
            rabbitmq_connection.close()

if __name__ == "__main__":
    main()

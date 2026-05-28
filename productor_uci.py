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
    print(f"[PUBLICADO] Exchange: '{exchange_name}' | Routing Key (Clave de Ruteo): '{routing_key}'")
    print(f"Payload (Cuerpo): {json.dumps(payload_dictionary, ensure_ascii=False)}")
    print("-" * 50)

def handle_manual_telemetry(amqp_channel):
    """
    Prompts the user for telemetry details and publishes it to the Topic exchange.
    """
    print("\n--- Publicar Telemetría de Signos Vitales (Topic Exchange) ---")
    bed_input = input("Ingrese el número de cama (ej. 05, 12) [Por defecto: 08]: ").strip() or "08"
    
    print("Seleccione el tipo de sensor:")
    print("1. Ritmo Cardíaco")
    print("2. Oxígeno (Saturación)")
    print("3. Temperatura")
    sensor_choice = input("Selección (1-3): ").strip()
    
    if sensor_choice == "1":
        sensor_type = "ritmo_cardiaco"
        metric_value = float(input("Ingrese ritmo cardíaco (bpm) [Por defecto: 72]: ").strip() or "72")
        unit = "bpm"
        # Evaluate severity
        if metric_value < 50 or metric_value > 120:
            severity_level = "critical"
            description = "¡Ritmo cardíaco anormal detectado!"
        elif metric_value < 60 or metric_value > 100:
            severity_level = "warning"
            description = "Ritmo cardíaco ligeramente fuera de rango."
        else:
            severity_level = "info"
            description = "Ritmo cardíaco normal."
            
    elif sensor_choice == "2":
        sensor_type = "oxigeno"
        metric_value = float(input("Ingrese saturación de oxígeno SpO2 (%) [Por defecto: 97]: ").strip() or "97")
        unit = "%"
        if metric_value < 90:
            severity_level = "critical"
            description = "¡Alerta de hipoxia! Niveles críticos de saturación de oxígeno."
        elif metric_value < 94:
            severity_level = "warning"
            description = "Niveles de oxígeno bajos."
        else:
            severity_level = "info"
            description = "Niveles de oxígeno normales."
            
    else:
        sensor_type = "temperatura"
        metric_value = float(input("Ingrese temperatura (°C) [Por defecto: 36.8]: ").strip() or "36.8")
        unit = "C"
        if metric_value > 38.5 or metric_value < 35.0:
            severity_level = "critical"
            description = "¡Alerta crítica de temperatura (fiebre alta o hipotermia)!"
        elif metric_value > 37.5 or metric_value < 36.0:
            severity_level = "warning"
            description = "Advertencia de fiebre moderada."
        else:
            severity_level = "info"
            description = "Temperatura corporal normal."

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
    print("\n--- Publicar Alerta de Severidad (Direct Exchange) ---")
    print("Seleccione el nivel de severidad:")
    print("1. INFO (Información general)")
    print("2. WARNING (Requiere atención)")
    print("3. CRITICAL (Requiere respuesta inmediata)")
    severity_choice = input("Selección (1-3): ").strip()
    
    severity_level = "info"
    if severity_choice == "2":
        severity_level = "warning"
    elif severity_choice == "3":
        severity_level = "critical"
        
    description = input("Ingrese el mensaje de la alerta: ").strip() or "Alerta de diagnóstico estándar"
    
    payload_dictionary = generate_medical_payload(
        bed_number="SISTEMA",
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
    print("\n--- Publicar Comunicado de Bioseguridad (Fanout Exchange) ---")
    description = input("Ingrese el comunicado de bioseguridad: ").strip() or "Se requiere verificación rutinaria de bioseguridad."
    
    payload_dictionary = generate_medical_payload(
        bed_number="TODOS",
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
    print("\n[INICIO] Iniciando simulación automática de telemetrías de pacientes en UCI...")
    print("Presione Ctrl+C para detener la simulación y regresar al menú.\n")
    
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
                    description = f"¡Límite crítico superado para {selected_sensor['type']}!"
                else:
                    metric_value = round(random.uniform(*selected_sensor["normal_range"]), 1)
                    severity_level = "info"
                    description = f"Lectura de telemetría normal para {selected_sensor['type']}."
                
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
                    "info": f"Actualización de cambio de turno de enfermería para cama {selected_bed}.",
                    "warning": f"Advertencia de bomba de infusión: bajo volumen restante en cama {selected_bed}.",
                    "critical": f"¡Electrodo de ECG desconectado en la cama {selected_bed}!"
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
                    "Fallo general de energía en el ala norte - generador de respaldo activo.",
                    "Protocolos de descontaminación de bioseguridad activados en Quirófano 2.",
                    "Mantenimiento programado de tuberías de agua en el ala UCI a las 22:00."
                ]
                
                payload_dictionary = generate_medical_payload(
                    bed_number="TODOS",
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
        print("\n[PARADA] Simulación automática detenida por el usuario.")

def main():
    """
    Main entry point for the producer simulation cli.
    """
    print("=" * 60)
    print("  SIMULADOR PRODUCTOR: Telemetría UCI y Alertas de Seguridad  ")
    print("=" * 60)
    
    # Establish connection and set up the channel
    try:
        rabbitmq_connection = config.get_rabbitmq_connection()
        amqp_channel = rabbitmq_connection.channel()
        
        # Ensure infrastructure exists before starting operations (Idempotence)
        config.setup_infrastructure(amqp_channel)
    except Exception as connection_exception:
        print(f"[FATAL] Inicialización de conexión abortada. {connection_exception}")
        sys.exit(1)
        
    try:
        while True:
            print("\nSeleccione una acción:")
            print("1. Publicar Telemetría manual de signos vitales (Topic -> uci.monitoreo)")
            print("2. Publicar Alerta manual de severidad (Direct -> uci.alertas)")
            print("3. Publicar Comunicado manual de bioseguridad (Fanout -> uci.bioseguridad)")
            print("4. Ejecutar bucle de simulación automática de UCI en tiempo real")
            print("5. Salir")
            
            main_menu_choice = input("Selección (1-5): ").strip()
            
            if main_menu_choice == "1":
                handle_manual_telemetry(amqp_channel)
            elif main_menu_choice == "2":
                handle_manual_alert(amqp_channel)
            elif main_menu_choice == "3":
                handle_manual_biosecurity(amqp_channel)
            elif main_menu_choice == "4":
                run_automated_simulation(amqp_channel)
            elif main_menu_choice == "5":
                print("Saliendo del simulador productor. ¡Adiós!")
                break
            else:
                print("[ADVERTENCIA] Selección no válida. Por favor elija del 1 al 5.")
                
    except KeyboardInterrupt:
        print("\nSaliendo del productor. Cerrando conexión.")
    finally:
        if rabbitmq_connection.is_open:
            rabbitmq_connection.close()

if __name__ == "__main__":
    main()

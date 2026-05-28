import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import json
import threading
import queue
import time
import random
import uuid
from datetime import datetime
import pika

import config

class UCIDashboardApp:
    """
    Main Application class for the UCI Graphical Monitoring Dashboard.
    Manages GUI thread, background AMQP threads, and queue communications.
    """
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("UCI Monitoreo Crítico & Seguridad - RabbitMQ Dashboard")
        self.root.geometry("1200x700")
        self.root.configure(bg="#1e1e24")
        
        # Thread-safe queue for communicating received messages from background threads to GUI thread
        self.message_update_queue = queue.Queue()
        
        # Flags to control background threads
        self.is_simulation_active = False
        self.is_running = True
        
        # AMQP connection objects
        self.producer_connection = None
        self.producer_channel = None
        self.medical_connection = None
        self.security_connection = None
        
        # Try establishing initial producer connection and setting up infrastructure
        self.initialize_rabbitmq()
        
        # Build UI layout
        self.setup_styles()
        self.create_widgets()
        
        # Launch background consumer threads
        self.start_consumer_threads()
        
        # Start periodic GUI queue poller
        self.root.after(100, self.poll_received_messages)
        
        # Handle graceful window closure
        self.root.protocol("WM_DELETE_WINDOW", self.on_close_app)

    def setup_styles(self):
        """
        Configures theme colors and font styles matching a dark premium aesthetic.
        """
        style = ttk.Style()
        style.theme_use("clam")
        
        # Dark style configurations
        style.configure(".", background="#1e1e24", foreground="#ffffff")
        style.configure("TLabel", background="#1e1e24", foreground="#ffffff", font=("Segoe UI", 10))
        style.configure("Header.TLabel", background="#1e1e24", foreground="#3f51b5", font=("Segoe UI", 14, "bold"))
        style.configure("SubHeader.TLabel", background="#2a2a35", foreground="#90caf9", font=("Segoe UI", 11, "bold"))
        
        style.configure("TFrame", background="#1e1e24")
        style.configure("Card.TFrame", background="#2a2a35", relief="flat")
        
        style.configure("TButton", background="#3f51b5", foreground="#ffffff", borderwidth=0, font=("Segoe UI", 10, "bold"))
        style.map("TButton", background=[("active", "#5c6bc0")])
        
        style.configure("Alert.TButton", background="#f44336", foreground="#ffffff", font=("Segoe UI", 10, "bold"))
        style.map("Alert.TButton", background=[("active", "#e53935")])
        
        style.configure("TEntry", fieldbackground="#1e1e24", foreground="#ffffff", bordercolor="#424242")
        style.configure("TCombobox", fieldbackground="#1e1e24", background="#1e1e24", foreground="#ffffff")

    def initialize_rabbitmq(self):
        """
        Connects and declares the AMQP topology. Displays graphical error popups if it fails.
        """
        try:
            self.producer_connection = config.get_rabbitmq_connection()
            self.producer_channel = self.producer_connection.channel()
            # Ensure topology exists idempotently
            config.setup_infrastructure(self.producer_channel)
        except Exception as connection_error:
            messagebox.showerror(
                "Error de Conexión a RabbitMQ",
                f"No se pudo inicializar la conexión con el servidor RabbitMQ en localhost:5672.\n\n"
                f"Detalle: {connection_error}\n\n"
                f"Asegúrese de que RabbitMQ esté encendido y que el Virtual Host '/uci_app' haya sido creado."
            )
            # Do not exit instantly, allow the GUI to open so they can inspect/retry

    def create_widgets(self):
        """
        Constructs the columns for the Producer, Medical Consumer, and Security Consumer.
        """
        # Outer main container frame
        main_layout_frame = ttk.Frame(self.root, padding=15)
        main_layout_frame.pack(fill="both", expand=True)
        
        # Split layout into three equal columns
        main_layout_frame.columnconfigure(0, weight=1, uniform="equal_cols")
        main_layout_frame.columnconfigure(1, weight=1, uniform="equal_cols")
        main_layout_frame.columnconfigure(2, weight=1, uniform="equal_cols")
        main_layout_frame.rowconfigure(0, weight=1)
        
        # -------------------------------------------------------------
        # COLUMN 1: PRODUCER CONTROL PANEL
        # -------------------------------------------------------------
        producer_card = ttk.Frame(main_layout_frame, style="Card.TFrame", padding=15)
        producer_card.grid(row=0, column=0, padx=10, sticky="nsew")
        
        title_label_producer = ttk.Label(producer_card, text="🏥 PANEL DE SIMULACIÓN UCI", style="Header.TLabel", background="#2a2a35")
        title_label_producer.pack(anchor="w", pady=(0, 15))
        
        # Sub-Section A: Vital Signs Telemetry (Topic)
        section_vitals_frame = ttk.LabelFrame(producer_card, text=" 🫀 Telemetría de Pacientes (Topic) ", padding=10, labelanchor="nw")
        section_vitals_frame.pack(fill="x", pady=5)
        
        grid_vitals = ttk.Frame(section_vitals_frame)
        grid_vitals.pack(fill="x")
        
        ttk.Label(grid_vitals, text="Número Cama:").grid(row=0, column=0, sticky="w", pady=2)
        self.cama_entry = ttk.Entry(grid_vitals, width=10)
        self.cama_entry.insert(0, "05")
        self.cama_entry.grid(row=0, column=1, sticky="w", padx=5, pady=2)
        
        ttk.Label(grid_vitals, text="Tipo Signo:").grid(row=1, column=0, sticky="w", pady=2)
        self.sensor_combobox = ttk.Combobox(grid_vitals, values=["ritmo_cardiaco", "oxigeno", "temperatura"], state="readonly", width=15)
        self.sensor_combobox.current(0)
        self.sensor_combobox.grid(row=1, column=1, sticky="w", padx=5, pady=2)
        
        ttk.Label(grid_vitals, text="Valor Medido:").grid(row=2, column=0, sticky="w", pady=2)
        self.valor_entry = ttk.Entry(grid_vitals, width=10)
        self.valor_entry.insert(0, "75.0")
        self.valor_entry.grid(row=2, column=1, sticky="w", padx=5, pady=2)
        
        send_vitals_button = ttk.Button(section_vitals_frame, text="Enviar Signo Vital (Topic)", command=self.send_manual_telemetry)
        send_vitals_button.pack(fill="x", pady=(10, 0))
        
        # Sub-Section B: Direct Alerts (Direct)
        section_alerts_frame = ttk.LabelFrame(producer_card, text=" ⚠️ Alertas de Gravedad (Direct) ", padding=10)
        section_alerts_frame.pack(fill="x", pady=10)
        
        grid_alerts = ttk.Frame(section_alerts_frame)
        grid_alerts.pack(fill="x")
        
        ttk.Label(grid_alerts, text="Severidad:").grid(row=0, column=0, sticky="w", pady=2)
        self.severity_combobox = ttk.Combobox(grid_alerts, values=["info", "warning", "critical"], state="readonly", width=12)
        self.severity_combobox.current(0)
        self.severity_combobox.grid(row=0, column=1, sticky="w", padx=5, pady=2)
        
        ttk.Label(grid_alerts, text="Mensaje:").grid(row=1, column=0, sticky="w", pady=2)
        self.alerta_mensaje_entry = ttk.Entry(grid_alerts, width=22)
        self.alerta_mensaje_entry.insert(0, "Electrodo de ECG suelto")
        self.alerta_mensaje_entry.grid(row=1, column=1, sticky="w", padx=5, pady=2)
        
        send_alert_button = ttk.Button(section_alerts_frame, text="Enviar Alerta Severa (Direct)", command=self.send_manual_alert)
        send_alert_button.pack(fill="x", pady=(10, 0))
        
        # Sub-Section C: Biosecurity (Fanout)
        section_biosecurity_frame = ttk.LabelFrame(producer_card, text=" 📢 Comunicado Bioseguridad (Fanout) ", padding=10)
        section_biosecurity_frame.pack(fill="x", pady=5)
        
        self.bioseguridad_mensaje_entry = ttk.Entry(section_biosecurity_frame)
        self.bioseguridad_mensaje_entry.insert(0, "Fallo de energía en Planta Norte")
        self.bioseguridad_mensaje_entry.pack(fill="x", pady=2)
        
        send_biosecurity_button = ttk.Button(section_biosecurity_frame, text="Difundir Comunicado (Fanout)", command=self.send_manual_biosecurity)
        send_biosecurity_button.pack(fill="x", pady=(8, 0))
        
        # Sub-Section D: Auto Simulation Checkbox
        self.auto_simulation_var = tk.BooleanVar(value=False)
        self.auto_checkbox = tk.Checkbutton(
            producer_card,
            text="Simulación Automática Real-Time",
            variable=self.auto_simulation_var,
            command=self.toggle_automated_simulation,
            bg="#2a2a35",
            fg="#ffffff",
            selectcolor="#1e1e24",
            activebackground="#2a2a35",
            activeforeground="#ffffff",
            font=("Segoe UI", 10, "bold")
        )
        self.auto_checkbox.pack(pady=(15, 0), anchor="center")
        
        # -------------------------------------------------------------
        # COLUMN 2: CLINICAL MONITOR (CONSUMER 1)
        # -------------------------------------------------------------
        medical_card = ttk.Frame(main_layout_frame, style="Card.TFrame", padding=15)
        medical_card.grid(row=0, column=1, padx=10, sticky="nsew")
        
        title_label_medical = ttk.Label(medical_card, text="🩺 MONITOR CLÍNICO DE UCI", style="Header.TLabel", background="#2a2a35")
        title_label_medical.pack(anchor="w", pady=(0, 10))
        
        info_label_medical = ttk.Label(
            medical_card,
            text="Escuchando telemetría de camas y alertas de nivel warning/critical.",
            wraplength=320,
            background="#2a2a35",
            foreground="#b0bec5",
            font=("Segoe UI", 9, "italic")
        )
        info_label_medical.pack(fill="x", pady=(0, 10))
        
        # Scrolled Text log area
        self.medical_log_box = scrolledtext.ScrolledText(
            medical_card,
            bg="#121214",
            fg="#eceff1",
            insertbackground="white",
            font=("Consolas", 9),
            relief="flat"
        )
        self.medical_log_box.pack(fill="both", expand=True)
        
        # Custom Tags for log formatting
        self.medical_log_box.tag_config("CRITICAL", foreground="#f44336", font=("Consolas", 9, "bold"))
        self.medical_log_box.tag_config("WARNING", foreground="#ffb300", font=("Consolas", 9, "bold"))
        self.medical_log_box.tag_config("INFO", foreground="#4caf50")
        self.medical_log_box.tag_config("ACK", foreground="#00acc1", font=("Consolas", 8, "italic"))
        
        # -------------------------------------------------------------
        # COLUMN 3: SECURITY & OPERATIONS MONITOR (CONSUMER 2)
        # -------------------------------------------------------------
        security_card = ttk.Frame(main_layout_frame, style="Card.TFrame", padding=15)
        security_card.grid(row=0, column=2, padx=10, sticky="nsew")
        
        title_label_security = ttk.Label(security_card, text="🚨 PANEL DE SEGURIDAD FISICA", style="Header.TLabel", background="#2a2a35")
        title_label_security.pack(anchor="w", pady=(0, 10))
        
        info_label_security = ttk.Label(
            security_card,
            text="Escuchando alertas de infraestructura críticas y notificaciones generales.",
            wraplength=320,
            background="#2a2a35",
            foreground="#b0bec5",
            font=("Segoe UI", 9, "italic")
        )
        info_label_security.pack(fill="x", pady=(0, 10))
        
        self.security_log_box = scrolledtext.ScrolledText(
            security_card,
            bg="#121214",
            fg="#eceff1",
            insertbackground="white",
            font=("Consolas", 9),
            relief="flat"
        )
        self.security_log_box.pack(fill="both", expand=True)
        
        # Custom Tags for log formatting
        self.security_log_box.tag_config("EMERGENCIA", foreground="#ffffff", background="#f44336", font=("Consolas", 10, "bold"))
        self.security_log_box.tag_config("BIOSEGURIDAD", foreground="#ffffff", background="#ff8f00", font=("Consolas", 9, "bold"))
        self.security_log_box.tag_config("ACK", foreground="#00acc1", font=("Consolas", 8, "italic"))

    # -----------------------------------------------------------------
    # AMQP PRODUCER LOGIC
    # -----------------------------------------------------------------
    def send_manual_telemetry(self):
        """
        Publishes vital signs telemetry to the Topic exchange.
        """
        if not self.producer_channel or not self.producer_channel.is_open:
            messagebox.showerror("Error", "No hay conexión activa con RabbitMQ.")
            return
            
        cama = self.cama_entry.get().strip() or "08"
        sensor = self.sensor_combobox.get()
        try:
            valor = float(self.valor_entry.get().strip())
        except ValueError:
            messagebox.showerror("Error de entrada", "El valor medido debe ser un número válido.")
            return
            
        # Standard clinical evaluations
        unit = "bpm" if sensor == "ritmo_cardiaco" else "%" if sensor == "oxigeno" else "C"
        severity = "info"
        description = "Signos estables."
        
        if sensor == "ritmo_cardiaco":
            if valor < 50 or valor > 120:
                severity = "critical"
                description = "¡Ritmo cardíaco anormal detectado!"
            elif valor < 60 or valor > 100:
                severity = "warning"
                description = "Ritmo cardíaco ligeramente fuera de rango."
        elif sensor == "oxigeno":
            if valor < 90:
                severity = "critical"
                description = "¡Alerta de hipoxia! Saturación de oxígeno crítica."
            elif valor < 94:
                severity = "warning"
                description = "Bajo nivel de saturación."
        else: # temperatura
            if valor > 38.5 or valor < 35.0:
                severity = "critical"
                description = "¡Alerta crítica de temperatura (fiebre o hipotermia)!"
            elif valor > 37.5 or valor < 36.0:
                severity = "warning"
                description = "Alerta de fiebre moderada."

        routing_key = f"cama.{cama}.{sensor}"
        payload = {
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "bed_number": cama,
            "sensor_type": sensor,
            "metric_value": valor,
            "measurement_unit": unit,
            "severity_level": severity,
            "description": description
        }
        
        try:
            self.publish_to_rabbitmq(config.EXCHANGE_MONITORING, routing_key, payload)
        except Exception as publish_error:
            messagebox.showerror("Error al enviar", f"Falló la publicación del mensaje: {publish_error}")

    def send_manual_alert(self):
        """
        Publishes custom system notifications to the Direct exchange.
        """
        if not self.producer_channel or not self.producer_channel.is_open:
            messagebox.showerror("Error", "No hay conexión activa con RabbitMQ.")
            return
            
        severity = self.severity_combobox.get()
        mensaje = self.alerta_mensaje_entry.get().strip() or "Mensaje de diagnóstico de red"
        
        payload = {
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "bed_number": "SISTEMA",
            "sensor_type": "alerta_directa",
            "metric_value": 0.0,
            "measurement_unit": "N/A",
            "severity_level": severity,
            "description": mensaje
        }
        
        try:
            self.publish_to_rabbitmq(config.EXCHANGE_ALERTS, severity, payload)
        except Exception as publish_error:
            messagebox.showerror("Error al enviar", f"Falló la publicación del mensaje: {publish_error}")

    def send_manual_biosecurity(self):
        """
        Broadcasts biosecurity messages to the Fanout exchange.
        """
        if not self.producer_channel or not self.producer_channel.is_open:
            messagebox.showerror("Error", "No hay conexión activa con RabbitMQ.")
            return
            
        mensaje = self.bioseguridad_mensaje_entry.get().strip() or "Control de acceso rutinario activo"
        
        payload = {
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "bed_number": "TODOS",
            "sensor_type": "bioseguridad",
            "metric_value": 0.0,
            "measurement_unit": "N/A",
            "severity_level": "warning",
            "description": mensaje
        }
        
        try:
            # Fanout exchange routing key is empty string
            self.publish_to_rabbitmq(config.EXCHANGE_BIOSECURITY, "", payload)
        except Exception as publish_error:
            messagebox.showerror("Error al enviar", f"Falló la publicación del mensaje: {publish_error}")

    def publish_to_rabbitmq(self, exchange, routing_key, payload):
        """
        Serializes and basic_publishes to RabbitMQ.
        """
        serialized = json.dumps(payload, indent=2)
        message_properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2
        )
        self.producer_channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=serialized,
            properties=message_properties
        )

    def toggle_automated_simulation(self):
        """
        Toggles state of automated generator background thread loop.
        """
        if self.auto_simulation_var.get():
            self.is_simulation_active = True
            simulation_thread = threading.Thread(target=self.run_background_simulation_loop, daemon=True)
            simulation_thread.start()
        else:
            self.is_simulation_active = False

    def run_background_simulation_loop(self):
        """
        Background loop simulating readings and emergencies periodically.
        """
        active_sensors = [
            {"type": "ritmo_cardiaco", "unit": "bpm", "normal_range": (60, 95), "critical_range": (40, 140)},
            {"type": "oxigeno", "unit": "%", "normal_range": (95, 100), "critical_range": (80, 89)},
            {"type": "temperatura", "unit": "C", "normal_range": (36.2, 37.2), "critical_range": (34.0, 40.0)}
        ]
        bed_identifiers = ["01", "02", "03", "04", "05"]
        
        while self.is_simulation_active and self.is_running:
            if not self.producer_channel or not self.producer_channel.is_open:
                time.sleep(1)
                continue
                
            event_probability = random.random()
            try:
                if event_probability < 0.70:
                    # Telemetry (Topic)
                    cama = random.choice(bed_identifiers)
                    sensor = random.choice(active_sensors)
                    
                    is_abnormal = random.random() < 0.12
                    if is_abnormal:
                        val = round(random.uniform(*sensor["critical_range"]), 1)
                        severity = "critical"
                        desc = f"¡Límite crítico superado para {sensor['type']}!"
                    else:
                        val = round(random.uniform(*sensor["normal_range"]), 1)
                        severity = "info"
                        desc = f"Lectura de telemetría normal para {sensor['type']}."
                        
                    routing_key = f"cama.{cama}.{sensor['type']}"
                    payload = generate_medical_payload(cama, sensor["type"], val, sensor["unit"], severity, desc)
                    self.publish_to_rabbitmq(config.EXCHANGE_MONITORING, routing_key, payload)
                    
                elif event_probability < 0.90:
                    # Direct Alerts
                    severity = random.choice(["info", "warning", "critical"])
                    cama = random.choice(bed_identifiers)
                    descriptions = {
                        "info": f"Cambio de turno rutinario para cama {cama}.",
                        "warning": f"Advertencia de bomba de infusión: bajo volumen restante en cama {cama}.",
                        "critical": f"¡Electrodo de ECG desconectado en cama {cama}!"
                    }
                    payload = generate_medical_payload(cama, "alerta_directa", 0.0, "N/A", severity, descriptions[severity])
                    self.publish_to_rabbitmq(config.EXCHANGE_ALERTS, severity, payload)
                    
                else:
                    # Biosecurity Broadcaster (Fanout)
                    notices = [
                        "Fallo de energía en Ala Norte - generador de respaldo activo.",
                        "Protocolos de descontaminación de bioseguridad en Quirófano 2.",
                        "Mantenimiento programado de tuberías en UCI a las 22:00."
                    ]
                    payload = generate_medical_payload("TODOS", "bioseguridad", 0.0, "N/A", "critical", random.choice(notices))
                    self.publish_to_rabbitmq(config.EXCHANGE_BIOSECURITY, "", payload)
                    
            except Exception:
                # Silently catch background publish failures to avoid thread crashes
                pass
                
            time.sleep(3)

    # -----------------------------------------------------------------
    # BACKGROUND CONSUMER THREADS
    # -----------------------------------------------------------------
    def start_consumer_threads(self):
        """
        Spawns separate daemon threads for medical and security listeners.
        """
        medical_listener = threading.Thread(target=self.run_medical_consumer_listener, daemon=True)
        medical_listener.start()
        
        security_listener = threading.Thread(target=self.run_security_consumer_listener, daemon=True)
        security_listener.start()

    def run_medical_consumer_listener(self):
        """
        Listens to config.QUEUE_MEDICAL_MONITOR in a separate connection/thread.
        """
        try:
            self.medical_connection = config.get_rabbitmq_connection()
            channel = self.medical_connection.channel()
            config.setup_infrastructure(channel)
            channel.basic_qos(prefetch_count=1)
            
            def callback(ch, method, properties, body):
                try:
                    payload = json.loads(body.decode('utf-8'))
                    # Place in thread-safe queue to schedule GUI log printing
                    self.message_update_queue.put(("MEDICAL", method.routing_key, payload))
                    
                    # Simulate processing delay
                    time.sleep(0.5)
                    # Acknowledge message manually
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.message_update_queue.put(("MEDICAL_ACK", "", {}))
                except Exception:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            channel.basic_consume(queue=config.QUEUE_MEDICAL_MONITOR, on_message_callback=callback, auto_ack=False)
            channel.start_consuming()
        except Exception:
            # Let thread exit gracefully if connection fails or closes
            pass

    def run_security_consumer_listener(self):
        """
        Listens to config.QUEUE_GENERAL_NOTICES & config.QUEUE_CRITICAL_ALERTS in a separate thread.
        """
        try:
            self.security_connection = config.get_rabbitmq_connection()
            channel = self.security_connection.channel()
            config.setup_infrastructure(channel)
            channel.basic_qos(prefetch_count=1)
            
            def callback(ch, method, properties, body):
                try:
                    payload = json.loads(body.decode('utf-8'))
                    self.message_update_queue.put(("SECURITY", (method.exchange, method.routing_key), payload))
                    
                    time.sleep(0.5)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.message_update_queue.put(("SECURITY_ACK", "", {}))
                except Exception:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            channel.basic_consume(queue=config.QUEUE_GENERAL_NOTICES, on_message_callback=callback, auto_ack=False)
            channel.basic_consume(queue=config.QUEUE_CRITICAL_ALERTS, on_message_callback=callback, auto_ack=False)
            channel.start_consuming()
        except Exception:
            pass

    # -----------------------------------------------------------------
    # GUI UPDATE POLLEING LOGIC (RUNS ON MAIN GUI THREAD)
    # -----------------------------------------------------------------
    def poll_received_messages(self):
        """
        Checks the thread-safe queue for incoming logs and paints them to the GUI frames.
        """
        if not self.is_running:
            return
            
        try:
            while True:
                msg_type, identifier, payload = self.message_update_queue.get_nowait()
                
                if msg_type == "MEDICAL":
                    self.append_to_medical_log(identifier, payload)
                elif msg_type == "MEDICAL_ACK":
                    self.medical_log_box.insert(tk.END, "[ACK] Evento confirmado en RabbitMQ.\n\n", "ACK")
                    self.medical_log_box.see(tk.END)
                elif msg_type == "SECURITY":
                    self.append_to_security_log(identifier[0], identifier[1], payload)
                elif msg_type == "SECURITY_ACK":
                    self.security_log_box.insert(tk.END, "[ACK] Alerta de seguridad confirmada en RabbitMQ.\n\n", "ACK")
                    self.security_log_box.see(tk.END)
                    
                self.message_update_queue.task_done()
        except queue.Empty:
            pass
            
        # Re-schedule poller every 100 milliseconds
        self.root.after(100, self.poll_received_messages)

    def append_to_medical_log(self, routing_key, payload):
        """
        Appends a formatted clinical reading with color highlights.
        """
        severity = payload.get("severity_level", "info").upper()
        cama = payload.get("bed_number", "UNKNOWN")
        sensor = payload.get("sensor_type", "UNKNOWN")
        valor = payload.get("metric_value", 0.0)
        unit = payload.get("measurement_unit", "")
        desc = payload.get("description", "")
        timestamp = payload.get("timestamp", "").split("T")[-1].replace("Z", "")
        
        # Decide tag color based on severity levels
        text_tag = "INFO"
        if severity in ["CRITICAL", "CRÍTICA", "CRITICA"]:
            text_tag = "CRITICAL"
            header = f"🚨 ALERTA CRÍTICA [{timestamp}]"
        elif severity in ["WARNING", "ADVERTENCIA"]:
            text_tag = "WARNING"
            header = f"⚠️ ADVERTENCIA [{timestamp}]"
        else:
            header = f"📉 TELEMETRÍA [{timestamp}]"
            
        self.medical_log_box.insert(tk.END, f"{header}\n", text_tag)
        self.medical_log_box.insert(tk.END, f"  Cama: #{cama} | Sensor: {sensor}\n")
        self.medical_log_box.insert(tk.END, f"  Lectura: {valor} {unit}\n")
        self.medical_log_box.insert(tk.END, f"  Ruta: {routing_key}\n")
        self.medical_log_box.insert(tk.END, f"  Info: {desc}\n")
        self.medical_log_box.see(tk.END)

    def append_to_security_log(self, exchange, routing_key, payload):
        """
        Appends security alerts with distinct background tags.
        """
        severity = payload.get("severity_level", "info").upper()
        dispositivo = payload.get("sensor_type", "seguridad")
        desc = payload.get("description", "")
        timestamp = payload.get("timestamp", "").split("T")[-1].replace("Z", "")
        
        if exchange == config.EXCHANGE_BIOSECURITY:
            header = f"📢 COMUNICADO BIOSEGURIDAD [{timestamp}]\n"
            self.security_log_box.insert(tk.END, header, "BIOSEGURIDAD")
            self.security_log_box.insert(tk.END, f"  Origen: {exchange}\n")
            self.security_log_box.insert(tk.END, f"  Detalle: {desc}\n")
        else:
            header = f"💥 EMERGENCIA DE INFRAESTRUCTURA [{timestamp}]\n"
            self.security_log_box.insert(tk.END, header, "EMERGENCIA")
            self.security_log_box.insert(tk.END, f"  Gravedad: {severity} | Origen: {dispositivo}\n")
            self.security_log_box.insert(tk.END, f"  Detalle: {desc}\n")
            
        self.security_log_box.see(tk.END)

    # -----------------------------------------------------------------
    # DESTRUCTION & EXIT ACTIONS
    # -----------------------------------------------------------------
    def on_close_app(self):
        """
        Terminates background threads and closes open socket connections cleanly.
        """
        self.is_running = False
        self.is_simulation_active = False
        
        # Close open rabbitmq connections to stop consumer threads
        try:
            if self.producer_connection and self.producer_connection.is_open:
                self.producer_connection.close()
            if self.medical_connection and self.medical_connection.is_open:
                self.medical_connection.close()
            if self.security_connection and self.security_connection.is_open:
                self.security_connection.close()
        except Exception:
            pass
            
        self.root.destroy()

def generate_medical_payload(bed_number, sensor_type, metric_value, unit, severity_level, description):
    """
    Helper function to generate simulation JSON bodies matching the schema.
    """
    return {
        "message_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bed_number": bed_number,
        "sensor_type": sensor_type,
        "metric_value": metric_value,
        "measurement_unit": unit,
        "severity_level": severity_level,
        "description": description
    }

def main():
    root = tk.Tk()
    app = UCIDashboardApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()

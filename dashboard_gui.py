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
    Highly-polished GUI Dashboard for the UCI Queue Management System.
    Uses custom styles, thread-safe asynchronous updates, and responsive spacing.
    """
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("UCI Monitoreo Crítico & Seguridad - RabbitMQ Dashboard")
        self.root.geometry("1300x780")
        self.root.configure(bg="#0f1016") # Deep slate black
        
        # Thread-safe queue for asynchronous messages from consumers
        self.message_update_queue = queue.Queue()
        
        # Simulation flags
        self.is_simulation_active = False
        self.is_running = True
        
        # AMQP connection objects
        self.producer_connection = None
        self.producer_channel = None
        self.medical_connection = None
        self.security_connection = None
        
        # Color palette constants
        self.COLOR_BG = "#0f1016"          # Outer background
        self.COLOR_CARD = "#171923"        # Inside panels background
        self.COLOR_ACCENT = "#4f46e5"      # Modern indigo
        self.COLOR_ACCENT_HOVER = "#6366f1"
        self.COLOR_TEXT_MAIN = "#ffffff"   # High contrast text
        self.COLOR_TEXT_MUTED = "#a0aec0"  # Muted grey text
        
        self.COLOR_SUCCESS = "#10b981"     # Emerald Green
        self.COLOR_WARNING = "#f59e0b"     # Amber
        self.COLOR_DANGER = "#ef4444"      # Rose Red
        self.COLOR_LOG_BG = "#0b0c10"      # Console black
        
        # Configure RabbitMQ connection
        self.initialize_rabbitmq()
        
        # Initialize UI Components
        self.setup_styles()
        self.create_header()
        self.create_widgets()
        
        # Launch background consumers
        self.start_consumer_threads()
        
        # Start periodic GUI queue poller
        self.root.after(100, self.poll_received_messages)
        
        # Handle graceful window closure
        self.root.protocol("WM_DELETE_WINDOW", self.on_close_app)

    def setup_styles(self):
        """
        Customizes theme colors, entries, dropdowns and button styles.
        """
        style = ttk.Style()
        style.theme_use("clam")
        
        # Set default styles
        style.configure(".", background=self.COLOR_BG, foreground=self.COLOR_TEXT_MAIN)
        style.configure("TLabel", background=self.COLOR_BG, foreground=self.COLOR_TEXT_MAIN, font=("Segoe UI", 10))
        
        # Card style panels
        style.configure("Card.TFrame", background=self.COLOR_CARD, relief="flat")
        style.configure("CardLabel.TLabel", background=self.COLOR_CARD, foreground=self.COLOR_TEXT_MAIN, font=("Segoe UI", 10))
        style.configure("CardHeader.TLabel", background=self.COLOR_CARD, foreground=self.COLOR_TEXT_MAIN, font=("Segoe UI", 12, "bold"))
        style.configure("CardSubHeader.TLabel", background=self.COLOR_CARD, foreground=self.COLOR_TEXT_MUTED, font=("Segoe UI", 9, "italic"))
        
        # Custom input entries and comboboxes
        style.configure("TEntry", fieldbackground="#1f2232", foreground=self.COLOR_TEXT_MAIN, bordercolor="#2d3748")
        style.map("TEntry", bordercolor=[("focus", self.COLOR_ACCENT)])
        
        style.configure("TCombobox", fieldbackground="#1f2232", background="#1f2232", foreground=self.COLOR_TEXT_MAIN, arrowcolor=self.COLOR_TEXT_MAIN)
        style.map("TCombobox", fieldbackground=[("readonly", "#1f2232")], selectbackground=[("readonly", "#1f2232")])
        
        # Custom LabelFrames
        style.configure("TLabelframe", background=self.COLOR_CARD, bordercolor="#2d3748", relief="solid", borderwidth=1)
        style.configure("TLabelframe.Label", background=self.COLOR_CARD, foreground=self.COLOR_TEXT_MUTED, font=("Segoe UI", 9, "bold"))

    def initialize_rabbitmq(self):
        """
        Declares RabbitMQ topology. Fails gracefully displaying a messagebox warning.
        """
        try:
            self.producer_connection = config.get_rabbitmq_connection()
            self.producer_channel = self.producer_connection.channel()
            config.setup_infrastructure(self.producer_channel)
        except Exception as connection_error:
            messagebox.showerror(
                "Error de Conexión a RabbitMQ",
                f"No se pudo inicializar la conexión con el servidor RabbitMQ en localhost:5672.\n\n"
                f"Detalle: {connection_error}\n\n"
                f"Asegúrese de que RabbitMQ esté encendido y que el Virtual Host '/uci_app' haya sido creado."
            )

    def create_header(self):
        """
        Creates a clean status bar banner at the top of the GUI.
        """
        header_frame = tk.Frame(self.root, bg="#13141f", height=60, bd=0)
        header_frame.pack(fill="x", side="top")
        header_frame.pack_propagate(False)
        
        # Hospital Title Label
        title_label = tk.Label(
            header_frame,
            text="🏥  MONITOREO DE PACIENTES & SEGURIDAD UCI",
            fg=self.COLOR_TEXT_MAIN,
            bg="#13141f",
            font=("Segoe UI", 14, "bold")
        )
        title_label.pack(side="left", padx=20, pady=15)
        
        # RabbitMQ Connection Pulse Indicator
        status_sub_frame = tk.Frame(header_frame, bg="#13141f")
        status_sub_frame.pack(side="right", padx=20, pady=15)
        
        status_color = self.COLOR_SUCCESS if (self.producer_channel and self.producer_channel.is_open) else self.COLOR_DANGER
        status_text = "● RabbitMQ: Conectado (vhost: /uci_app)" if (self.producer_channel and self.producer_channel.is_open) else "● RabbitMQ: Desconectado"
        
        status_dot = tk.Label(
            status_sub_frame,
            text=status_text,
            fg=status_color,
            bg="#13141f",
            font=("Segoe UI", 10, "bold")
        )
        status_dot.pack()

    def create_widgets(self):
        """
        Main workspace layout. Builds the 3 structured column panels.
        """
        workspace_frame = ttk.Frame(self.root, padding=15)
        workspace_frame.pack(fill="both", expand=True)
        
        workspace_frame.columnconfigure(0, weight=4, uniform="cols") # Left simulator
        workspace_frame.columnconfigure(1, weight=5, uniform="cols") # Middle Clinical Log
        workspace_frame.columnconfigure(2, weight=5, uniform="cols") # Right Security Log
        workspace_frame.rowconfigure(0, weight=1)
        
        # -------------------------------------------------------------
        # COLUMN 1: SIMULATOR CONTROL CARD
        # -------------------------------------------------------------
        producer_panel = ttk.Frame(workspace_frame, style="Card.TFrame", padding=15)
        producer_panel.grid(row=0, column=0, padx=10, sticky="nsew")
        
        title_producer = ttk.Label(producer_panel, text="EMISOR DE EVENTOS UCI", style="CardHeader.TLabel")
        title_producer.pack(anchor="w", pady=(0, 2))
        
        subtitle_producer = ttk.Label(producer_panel, text="Simula la emisión de datos clínicos y alertas críticas.", style="CardSubHeader.TLabel")
        subtitle_producer.pack(anchor="w", pady=(0, 15))
        
        # Group 1: Vitals (Topic Exchange)
        vitals_group = ttk.LabelFrame(producer_panel, text=" TELEMETRÍA DE SIGNOS VITALES [TOPIC] ")
        vitals_group.pack(fill="x", pady=5)
        
        grid_vitals = ttk.Frame(vitals_group, padding=10)
        grid_vitals.pack(fill="x")
        grid_vitals.columnconfigure(0, weight=1)
        grid_vitals.columnconfigure(1, weight=1)
        
        ttk.Label(grid_vitals, text="Número Cama:", style="CardLabel.TLabel").grid(row=0, column=0, sticky="w", pady=4)
        self.cama_entry = ttk.Entry(grid_vitals, width=12, font=("Segoe UI", 9))
        self.cama_entry.insert(0, "05")
        self.cama_entry.grid(row=0, column=1, sticky="w", pady=4)
        
        ttk.Label(grid_vitals, text="Tipo Sensor:", style="CardLabel.TLabel").grid(row=1, column=0, sticky="w", pady=4)
        self.sensor_combobox = ttk.Combobox(grid_vitals, values=["ritmo_cardiaco", "oxigeno", "temperatura"], state="readonly", width=14, font=("Segoe UI", 9))
        self.sensor_combobox.current(0)
        self.sensor_combobox.grid(row=1, column=1, sticky="w", pady=4)
        
        ttk.Label(grid_vitals, text="Valor Medido:", style="CardLabel.TLabel").grid(row=2, column=0, sticky="w", pady=4)
        self.valor_entry = ttk.Entry(grid_vitals, width=12, font=("Segoe UI", 9))
        self.valor_entry.insert(0, "75.0")
        self.valor_entry.grid(row=2, column=1, sticky="w", pady=4)
        
        self.btn_send_vitals = self.create_flat_button(
            vitals_group, "Publicar Signo Vital", self.send_manual_telemetry, self.COLOR_SUCCESS
        )
        self.btn_send_vitals.pack(fill="x", padx=10, pady=(5, 10))
        
        # Group 2: Alerts (Direct Exchange)
        alerts_group = ttk.LabelFrame(producer_panel, text=" ALERTAS DIRECTAS POR GRAVEDAD [DIRECT] ")
        alerts_group.pack(fill="x", pady=10)
        
        grid_alerts = ttk.Frame(alerts_group, padding=10)
        grid_alerts.pack(fill="x")
        grid_alerts.columnconfigure(0, weight=1)
        grid_alerts.columnconfigure(1, weight=1)
        
        ttk.Label(grid_alerts, text="Severidad:", style="CardLabel.TLabel").grid(row=0, column=0, sticky="w", pady=4)
        self.severity_combobox = ttk.Combobox(grid_alerts, values=["info", "warning", "critical"], state="readonly", width=14, font=("Segoe UI", 9))
        self.severity_combobox.current(0)
        self.severity_combobox.grid(row=0, column=1, sticky="w", pady=4)
        
        ttk.Label(grid_alerts, text="Mensaje:", style="CardLabel.TLabel").grid(row=1, column=0, sticky="w", pady=4)
        self.alerta_mensaje_entry = ttk.Entry(grid_alerts, width=18, font=("Segoe UI", 9))
        self.alerta_mensaje_entry.insert(0, "Electrodo de ECG suelto")
        self.alerta_mensaje_entry.grid(row=1, column=1, sticky="w", pady=4)
        
        self.btn_send_alerts = self.create_flat_button(
            alerts_group, "Publicar Alerta Directa", self.send_manual_alert, self.COLOR_WARNING
        )
        self.btn_send_alerts.pack(fill="x", padx=10, pady=(5, 10))
        
        # Group 3: Biosecurity (Fanout Exchange)
        biosecurity_group = ttk.LabelFrame(producer_panel, text=" AVISOS GENERALES Y BIOSEGURIDAD [FANOUT] ")
        biosecurity_group.pack(fill="x", pady=5)
        
        frame_bio = ttk.Frame(biosecurity_group, padding=10)
        frame_bio.pack(fill="x")
        
        ttk.Label(frame_bio, text="Comunicado de Hospital:", style="CardLabel.TLabel").pack(anchor="w", pady=(0, 4))
        self.bioseguridad_mensaje_entry = ttk.Entry(frame_bio, font=("Segoe UI", 9))
        self.bioseguridad_mensaje_entry.insert(0, "Fallo de energía en Planta Norte")
        self.bioseguridad_mensaje_entry.pack(fill="x", pady=2)
        
        self.btn_send_biosecurity = self.create_flat_button(
            biosecurity_group, "Difundir Aviso general", self.send_manual_biosecurity, self.COLOR_DANGER
        )
        self.btn_send_biosecurity.pack(fill="x", padx=10, pady=(5, 10))
        
        # Group 4: Automated Simulation Checkbutton
        self.auto_simulation_var = tk.BooleanVar(value=False)
        self.auto_checkbox = tk.Checkbutton(
            producer_panel,
            text=" Activar Simulación Automática en Tiempo Real",
            variable=self.auto_simulation_var,
            command=self.toggle_automated_simulation,
            bg=self.COLOR_CARD,
            fg="#cbd5e0",
            selectcolor="#1a1c23",
            activebackground=self.COLOR_CARD,
            activeforeground=self.COLOR_TEXT_MAIN,
            font=("Segoe UI", 9, "bold")
        )
        self.auto_checkbox.pack(pady=(20, 0), anchor="center")

        # -------------------------------------------------------------
        # COLUMN 2: CLINICAL TELEMETRY MONITOR (CONSUMER 1)
        # -------------------------------------------------------------
        medical_panel = ttk.Frame(workspace_frame, style="Card.TFrame", padding=15)
        medical_panel.grid(row=0, column=1, padx=10, sticky="nsew")
        
        title_med = ttk.Label(medical_panel, text="MONITOR CLÍNICO DE UCI", style="CardHeader.TLabel")
        title_med.pack(anchor="w", pady=(0, 2))
        
        sub_med = ttk.Label(medical_panel, text="Registro en tiempo real de signos de pacientes.", style="CardSubHeader.TLabel")
        sub_med.pack(anchor="w", pady=(0, 12))
        
        # Scrolled Text configuration with customized layout
        self.medical_log_box = scrolledtext.ScrolledText(
            medical_panel,
            bg=self.COLOR_LOG_BG,
            fg="#e2e8f0",
            insertbackground="white",
            font=("Consolas", 9),
            relief="flat",
            bd=0,
            padx=10,
            pady=10,
            spacing1=3,
            spacing2=2
        )
        self.medical_log_box.pack(fill="both", expand=True)
        
        # Log entry highlighting tags
        self.medical_log_box.tag_config("CRITICAL", foreground="#f87171", font=("Consolas", 9, "bold"))
        self.medical_log_box.tag_config("WARNING", foreground="#fbbf24", font=("Consolas", 9, "bold"))
        self.medical_log_box.tag_config("INFO", foreground="#34d399")
        self.medical_log_box.tag_config("ACK", foreground="#38bdf8", font=("Consolas", 8, "italic"))

        # -------------------------------------------------------------
        # COLUMN 3: SECURITY & INFRASTRUCTURE MONITOR (CONSUMER 2)
        # -------------------------------------------------------------
        security_panel = ttk.Frame(workspace_frame, style="Card.TFrame", padding=15)
        security_panel.grid(row=0, column=2, padx=10, sticky="nsew")
        
        title_sec = ttk.Label(security_panel, text="DEPARTAMENTO DE SEGURIDAD", style="CardHeader.TLabel")
        title_sec.pack(anchor="w", pady=(0, 2))
        
        sub_sec = ttk.Label(security_panel, text="Alarmas de infraestructura y avisos generales.", style="CardSubHeader.TLabel")
        sub_sec.pack(anchor="w", pady=(0, 12))
        
        self.security_log_box = scrolledtext.ScrolledText(
            security_panel,
            bg=self.COLOR_LOG_BG,
            fg="#e2e8f0",
            insertbackground="white",
            font=("Consolas", 9),
            relief="flat",
            bd=0,
            padx=10,
            pady=10,
            spacing1=3,
            spacing2=2
        )
        self.security_log_box.pack(fill="both", expand=True)
        
        self.security_log_box.tag_config("EMERGENCIA", foreground="#ffffff", background="#ef4444", font=("Consolas", 9, "bold"))
        self.security_log_box.tag_config("BIOSEGURIDAD", foreground="#ffffff", background="#f59e0b", font=("Consolas", 9, "bold"))
        self.security_log_box.tag_config("ACK", foreground="#38bdf8", font=("Consolas", 8, "italic"))

    def create_flat_button(self, parent, text, command, bg_color):
        """
        Creates a custom stylized flat button with smooth hover animation.
        """
        btn = tk.Button(
            parent,
            text=text,
            command=command,
            bg=bg_color,
            fg="#ffffff",
            activebackground=bg_color,
            activeforeground="#ffffff",
            relief="flat",
            font=("Segoe UI", 9, "bold"),
            bd=0,
            cursor="hand2",
            pady=6
        )
        
        # Hover effect functions
        def on_enter(event):
            btn.config(bg=self.lighten_color(bg_color))
        def on_leave(event):
            btn.config(bg=bg_color)
            
        btn.bind("<Enter>", on_enter)
        btn.bind("<Leave>", on_leave)
        return btn

    def lighten_color(self, hex_color):
        """
        Calculates a slightly lighter version of a color for the hover animation.
        """
        hex_color = hex_color.lstrip('#')
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        # Increase values slightly
        lighter_rgb = tuple(min(255, int(channel * 1.15)) for channel in rgb)
        return '#{:02x}{:02x}{:02x}'.format(*lighter_rgb)

    # -----------------------------------------------------------------
    # AMQP PRODUCER LOGIC
    # -----------------------------------------------------------------
    def send_manual_telemetry(self):
        if not self.producer_channel or not self.producer_channel.is_open:
            messagebox.showerror("Error de Conexión", "No hay conexión activa con el servidor RabbitMQ.")
            return
            
        cama = self.cama_entry.get().strip() or "08"
        sensor = self.sensor_combobox.get()
        try:
            valor = float(self.valor_entry.get().strip())
        except ValueError:
            messagebox.showerror("Entrada incorrecta", "El valor medido debe ser numérico.")
            return
            
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
                description = "Niveles bajos de oxígeno."
        else: # temperatura
            if valor > 38.5 or valor < 35.0:
                severity = "critical"
                description = "¡Alerta crítica de temperatura (fiebre alta o hipotermia)!"
            elif valor > 37.5 or valor < 36.0:
                severity = "warning"
                description = "Advertencia de fiebre moderada."

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
        except Exception as error:
            messagebox.showerror("Error de envío", f"No se pudo publicar: {error}")

    def send_manual_alert(self):
        if not self.producer_channel or not self.producer_channel.is_open:
            messagebox.showerror("Error de Conexión", "No hay conexión activa con el servidor RabbitMQ.")
            return
            
        severity = self.severity_combobox.get()
        mensaje = self.alerta_mensaje_entry.get().strip() or "Alerta de diagnóstico estándar"
        
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
        except Exception as error:
            messagebox.showerror("Error de envío", f"No se pudo publicar: {error}")

    def send_manual_biosecurity(self):
        if not self.producer_channel or not self.producer_channel.is_open:
            messagebox.showerror("Error de Conexión", "No hay conexión activa con el servidor RabbitMQ.")
            return
            
        mensaje = self.bioseguridad_mensaje_entry.get().strip() or "Se requiere verificación de bioseguridad"
        
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
            self.publish_to_rabbitmq(config.EXCHANGE_BIOSECURITY, "", payload)
        except Exception as error:
            messagebox.showerror("Error de envío", f"No se pudo publicar: {error}")

    def publish_to_rabbitmq(self, exchange, routing_key, payload):
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
        if self.auto_simulation_var.get():
            self.is_simulation_active = True
            sim_thread = threading.Thread(target=self.run_background_simulation, daemon=True)
            sim_thread.start()
        else:
            self.is_simulation_active = False

    def run_background_simulation(self):
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
                        "warning": f"Advertencia de bomba de infusión: bajo volumen en cama {cama}.",
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
                pass
                
            time.sleep(3)

    # -----------------------------------------------------------------
    # BACKGROUND CONSUMERS LISTENING
    # -----------------------------------------------------------------
    def start_consumer_threads(self):
        medical_thread = threading.Thread(target=self.run_medical_consumer_listener, daemon=True)
        medical_thread.start()
        
        security_thread = threading.Thread(target=self.run_security_consumer_listener, daemon=True)
        security_thread.start()

    def run_medical_consumer_listener(self):
        try:
            self.medical_connection = config.get_rabbitmq_connection()
            channel = self.medical_connection.channel()
            config.setup_infrastructure(channel)
            channel.basic_qos(prefetch_count=1)
            
            def callback(ch, method, properties, body):
                try:
                    payload = json.loads(body.decode('utf-8'))
                    self.message_update_queue.put(("MEDICAL", method.routing_key, payload))
                    
                    time.sleep(0.5)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.message_update_queue.put(("MEDICAL_ACK", "", {}))
                except Exception:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            channel.basic_consume(queue=config.QUEUE_MEDICAL_MONITOR, on_message_callback=callback, auto_ack=False)
            channel.start_consuming()
        except Exception:
            pass

    def run_security_consumer_listener(self):
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
    # GUI ASYNCHRONOUS POLLER
    # -----------------------------------------------------------------
    def poll_received_messages(self):
        if not self.is_running:
            return
            
        try:
            while True:
                msg_type, identifier, payload = self.message_update_queue.get_nowait()
                
                if msg_type == "MEDICAL":
                    self.append_to_medical_log(identifier, payload)
                elif msg_type == "MEDICAL_ACK":
                    self.medical_log_box.insert(tk.END, "[✓ ACK] Mensaje procesado y retirado de cola.\n\n", "ACK")
                    self.medical_log_box.see(tk.END)
                elif msg_type == "SECURITY":
                    self.append_to_security_log(identifier[0], identifier[1], payload)
                elif msg_type == "SECURITY_ACK":
                    self.security_log_box.insert(tk.END, "[✓ ACK] Alerta física confirmada y registrada.\n\n", "ACK")
                    self.security_log_box.see(tk.END)
                    
                self.message_update_queue.task_done()
        except queue.Empty:
            pass
            
        self.root.after(100, self.poll_received_messages)

    def append_to_medical_log(self, routing_key, payload):
        severity = payload.get("severity_level", "info").upper()
        cama = payload.get("bed_number", "UNKNOWN")
        sensor = payload.get("sensor_type", "UNKNOWN")
        valor = payload.get("metric_value", 0.0)
        unit = payload.get("measurement_unit", "")
        desc = payload.get("description", "")
        timestamp = payload.get("timestamp", "").split("T")[-1].replace("Z", "")
        
        text_tag = "INFO"
        if severity in ["CRITICAL", "CRÍTICA", "CRITICA"]:
            text_tag = "CRITICAL"
            header = f"● 🚨 [ALERTA CLÍNICA CRÍTICA] @ {timestamp}\n"
        elif severity in ["WARNING", "ADVERTENCIA"]:
            text_tag = "WARNING"
            header = f"● ⚠️ [ADVERTENCIA CLÍNICA] @ {timestamp}\n"
        else:
            header = f"● ℹ [TELEMETRÍA RECIBIDA] @ {timestamp}\n"
            
        self.medical_log_box.insert(tk.END, header, text_tag)
        self.medical_log_box.insert(tk.END, f"  Paciente: Cama #{cama} | Sensor: {sensor}\n")
        self.medical_log_box.insert(tk.END, f"  Medición: {valor} {unit} | Clave Ruteo: {routing_key}\n")
        self.medical_log_box.insert(tk.END, f"  Detalle:  {desc}\n")
        self.medical_log_box.see(tk.END)

    def append_to_security_log(self, exchange, routing_key, payload):
        severity = payload.get("severity_level", "info").upper()
        sensor = payload.get("sensor_type", "seguridad")
        desc = payload.get("description", "")
        timestamp = payload.get("timestamp", "").split("T")[-1].replace("Z", "")
        
        if exchange == config.EXCHANGE_BIOSECURITY:
            header = f"● 📢 [COMUNICADO DE BIOSEGURIDAD] @ {timestamp}\n"
            self.security_log_box.insert(tk.END, header, "BIOSEGURIDAD")
            self.security_log_box.insert(tk.END, f"  Origen:  Exchange Fanout ('{exchange}')\n")
            self.security_log_box.insert(tk.END, f"  Detalle: {desc}\n")
        else:
            header = f"● 🔥 [EMERGENCIA DE INFRAESTRUCTURA] @ {timestamp}\n"
            self.security_log_box.insert(tk.END, header, "EMERGENCIA")
            self.security_log_box.insert(tk.END, f"  Gravedad: {severity} | Origen: {sensor}\n")
            self.security_log_box.insert(tk.END, f"  Detalle:  {desc}\n")
            
        self.security_log_box.see(tk.END)

    # -----------------------------------------------------------------
    # SHUTDOWN ACTION
    # -----------------------------------------------------------------
    def on_close_app(self):
        self.is_running = False
        self.is_simulation_active = False
        
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

# Tarea: Arquitectura de Microservicios con Patrón de Buzón (Mailbox)

Este proyecto implementa una arquitectura distribuida basada en microservicios independientes que se comunican de forma **asíncrona** utilizando un gestor de colas (RabbitMQ) y aplicando las mejores prácticas de diseño arquitectónico (**SOLID** y **Puertos y Adaptadores / Arquitectura Hexagonal**).

El caso de uso real desarrollado corresponde a un **Sistema de Alertas de Telemetría Médica (Monitoreo de Constantes Vitales)**.

---

## Características Arquitectónicas

### 1. Despliegue Autónomo y Contenedores (Desplegarse Solo)
Cada microservicio cuenta con su propio entorno aislado configurado en un `Dockerfile` (Python 3.11-slim) y se orquestan juntos mediante `docker-compose.yml`.
*   **Productor (API de Telemetría):** Corre en el puerto `8001`.
*   **Consumidor (Gestor de Alertas & Dashboard):** Corre en el puerto `8002`.
*   **Buzón (RabbitMQ Broker):** Corre en el puerto `5672` (AMQP) y `15672` (Panel de Administración).

### 2. Comunicación Asíncrona Garantizada (Buzón de Mensajes)
El Productor no se conecta directamente con el Consumidor. Cuando un sensor envía una alerta vital:
1.  El Productor valida la alerta.
2.  La encola de manera asíncrona en la cola persistente `mailbox_queue` de **RabbitMQ**.
3.  El Consumidor escucha la cola mediante un demonio en segundo plano (*Background Thread*) y procesa los mensajes conforme ingresan. Si el Consumidor se cae, los mensajes quedan seguros en el buzón.

### 3. Almacenamiento Propio (Database per Service)
Cada microservicio es dueño absoluto de su almacenamiento, garantizando un acoplamiento mínimo:
*   **Productor:** Escribe en su base de datos local `producer/data/producer.db` (historial de despachos).
*   **Consumidor:** Escribe en su base de datos local `consumer/data/consumer.db` (historial de alertas recibidas).
*(Las bases de datos se persisten en el Host mediante volúmenes de Docker).*

### 4. Exposición de APIs y Dashboard de Monitoreo
*   El microservicio Consumidor expone la API `GET /alerts/consumed` que retorna los mensajes que consumió de la cola.
*   Además, el Consumidor sirve una **interfaz gráfica web en vivo (`GET /`)** hecha en HTML/CSS oscuro con animación y refresco dinámico automático cada 3 segundos (utilizando Polling de JS), mostrando las constantes de los pacientes en tiempo real con colores de severidad (Rojo para CRITICAL, Naranja para WARNING, Verde/Azul para INFO).

---

## Cumplimiento de Principios de Diseño

### SOLID & Arquitectura Hexagonal (Ports & Adapters)
Para evitar el código espagueti (*tallarines*), el código fuente está dividido en capas estrictamente en inglés:
*   **Domain (Domain Object & Ports):** Contiene los modelos lógicos de negocio y las interfaces abstractas (`ABC` en Python). Esta capa no depende de ningún framework, base de datos o cola de mensajería (Inversión de Dependencias - **DIP**).
*   **Infrastructure (Adapters):** Implementa las interfaces abstractas.
    *   `db_adapter.py`: Implementa el acceso a base de datos SQLite con SQLAlchemy.
    *   `broker_adapter.py`: Implementa la conexión y publicación/suscripción a RabbitMQ usando la librería `pika`.
*   **Main (Router & Startup):** Inicializa la aplicación e inyecta los adaptadores a los puertos correspondientes.
*   **Single Responsibility Principle (SRP):** Cada clase tiene una sola razón para cambiar (los controladores de ruta manejan HTTP, las interfaces de repositorio manejan persistencia, la lógica del broker maneja la cola).
*   **Variables y Comentarios en Inglés:** Todo el código fuente está escrito en inglés por estándares profesionales de software.

---

## Requisitos Previos

*   [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y en ejecución en el sistema.
*   [Docker Compose](https://docs.docker.com/compose/) habilitado.

---

## Instrucciones de Ejecución

1.  Abre una terminal de PowerShell o CMD en la raíz de este directorio (`C:\Users\jeanc\.gemini\antigravity\scratch\microservices_mailbox_assignment`).
2.  Levanta los contenedores compilando las imágenes desde cero:
    ```bash
    docker compose up --build
    ```
    *Nota: Ambos servicios implementan reconexión y espera con reintento automático para garantizar resiliencia si RabbitMQ tarda en iniciar.*

3.  Una vez iniciados los servicios, puedes acceder a:
    *   **Dashboard Web Interactivo:** [http://localhost:8002/](http://localhost:8002/) (Panel del Consumidor).
    *   **FastAPI Docs (Productor):** [http://localhost:8001/docs](http://localhost:8001/docs)
    *   **FastAPI Docs (Consumidor):** [http://localhost:8002/docs](http://localhost:8002/docs)
    *   **RabbitMQ Dashboard:** [http://localhost:15672](http://localhost:15672) (Usuario: `guest` / Contraseña: `guest`).

---

## Instrucciones de Prueba (Paso a Paso)

### 1. Enviar una Alerta Vital (Productor)
Usa una herramienta como Postman o ejecuta el siguiente comando en otra consola para simular una alerta crítica de ritmo cardíaco (Taquicardia en paciente):

**En Windows (PowerShell):**
```powershell
Invoke-RestMethod -Uri "http://localhost:8001/alerts/send" -Method Post -ContentType "application/json" -Body '{"patient_id": "PAT-9842", "patient_name": "John Doe", "heart_rate": 145, "blood_pressure": "150/95", "alert_level": "CRITICAL"}'
```

**O usando `curl` (Git Bash / CMD):**
```bash
curl -X POST "http://localhost:8001/alerts/send" -H "Content-Type: application/json" -d '{"patient_id": "PAT-9842", "patient_name": "John Doe", "heart_rate": 145, "blood_pressure": "150/95", "alert_level": "CRITICAL"}'
```

### 2. Verificar la Comunicación Asíncrona
*   El Productor responderá con un JSON indicando que la alerta se registró localmente y se publicó en la cola: `"status": "sent"`.
*   Abre el **Dashboard Web** ([http://localhost:8002/](http://localhost:8002/)). Verás que la tarjeta del paciente "John Doe" aparece automáticamente destacada con un borde rojo parpadeante e indicando la latencia en segundos (usualmente centésimas de segundo) del viaje por la cola RabbitMQ.
*   Puedes enviar alertas de nivel `WARNING` (ej. ritmo de `100` bpm) o `INFO` (ej. ritmo de `75` bpm) para observar cómo cambia dinámicamente el panel con su respectiva colorimetría HSL.

### 3. Verificar Almacenamiento Propio e Independiente
Revisa las subcarpetas del proyecto en tu máquina:
*   En `producer/data/producer.db` se habrá creado el archivo SQLite del Productor.
*   En `consumer/data/consumer.db` se habrá creado el archivo SQLite del Consumidor.
Las estructuras de base de datos y la información están completamente aisladas y seguras.

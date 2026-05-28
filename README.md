# Proyecto Académico: Gestor de Alertas y Monitoreo Crítico en UCI (RabbitMQ + Python)

Este proyecto académico ha sido desarrollado para la materia de **Diseño y Arquitectura de Software**, implementando el patrón **Productor-Consumidor** sobre sistemas distribuidos mediante **RabbitMQ** y la librería `pika` de **Python**.

## 1. Arquitectura de Mensajería del Sistema

El sistema emula una Unidad de Cuidados Intensivos (UCI) y seguridad física hospitalaria. Los mensajes fluyen por tres tipos de Exchanges de RabbitMQ con diferentes reglas de enrutamiento hacia sus colas específicas:

```
                                      [ Virtual Host: /uci_app ]
                                      
                                 +---> EXCHANGE (Fanout): uci.bioseguridad --(Broadcast)----> COLA: cola.notificaciones_generales ---> CONSUMIDOR: consumidor_seguridad.py
                                 |
                                 |                                           +--[critical]---> COLA: cola.alertas_criticas ---------> CONSUMIDOR: consumidor_seguridad.py
PRODUCTOR: productor_uci.py -----+---> EXCHANGE (Direct): uci.alertas -------+
                                 |                                           +--[warning]----+
                                 |                                           +--[critical]---+--> COLA: cola.monitoreo_medico ----------> CONSUMIDOR: consumidor_medico.py
                                 |
                                 |                                           +--[cama.*.ritmo_cardiaco]--+
                                 +---> EXCHANGE (Topic): uci.monitoreo ------+--[cama.*.oxigeno]---------+--> COLA: cola.monitoreo_medico ---> CONSUMIDOR: consumidor_medico.py
                                                                             +--[cama.*.temperatura]-----+
```

### Exchanges (Intercambiadores)
1. **`uci.bioseguridad` (Fanout)**:
   - *Propósito:* Difundir avisos generales y de bioseguridad que deben llegar a todo el hospital (ej. "Corte de agua en el ala norte").
   - *Enrutamiento:* Difunde a todas las colas enlazadas sin importar la routing key.
2. **`uci.alertas` (Direct)**:
   - *Propósito:* Enrutar alertas específicas basadas en la severidad exacta de los incidentes.
   - *Routing Keys:* `info`, `warning`, `critical`.
3. **`uci.monitoreo` (Topic)**:
   - *Propósito:* Enrutar datos de telemetría de signos vitales médicos utilizando wildcards/comodines.
   - *Estructura de la clave:* `cama.<numero_cama>.<tipo_sensor>` (ej. `cama.05.ritmo_cardiaco`).

### Colas y Enlaces (Queues & Bindings)
- **`cola.monitoreo_medico`** (Consumidor Clínico):
  - Enlazada a `uci.monitoreo` (Topic) con: `cama.*.ritmo_cardiaco`, `cama.*.oxigeno`, `cama.*.temperatura` para recibir telemetría.
  - Enlazada a `uci.alertas` (Direct) con routing keys `warning` y `critical` para reaccionar a incidentes clínicos graves.
- **`cola.notificaciones_generales`** (Consumidor de Seguridad):
  - Enlazada a `uci.bioseguridad` (Fanout) para recibir noticias de bioseguridad generales.
- **`cola.alertas_criticas`** (Consumidor de Seguridad):
  - Enlazada a `uci.alertas` (Direct) con routing key `critical` para alertas críticas de infraestructura.

---

## 2. Requisitos y Preparación del Entorno

### Paso A: Levantar RabbitMQ (Recomendado vía Docker)
Si tienes Docker instalado, ejecuta el siguiente comando en la terminal para descargar e iniciar una instancia local de RabbitMQ con la interfaz web de administración activada:

```bash
docker run -d --name rabbitmq-uci -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### Paso B: Creación del Virtual Host `/uci_app` (¡CRÍTICO!)
El sistema requiere operar dentro del VHost `/uci_app` para aislar los recursos académicos. 
Tienes dos formas de crearlo:

#### Opción 1: Desde la interfaz de Administración Web (Fácil)
1. Abre tu navegador e ingresa a: `http://localhost:15672` (Usuario: `guest` / Contraseña: `guest`).
2. Dirígete a la pestaña superior **Admin**.
3. En la barra lateral derecha, haz clic en **Virtual Hosts**.
4. En la sección "Add a new virtual host", ingresa `/uci_app` en el campo *Name*.
5. Haz clic en el botón **Add virtual host**.

#### Opción 2: Desde la consola Docker / CLI de RabbitMQ
Ejecuta los siguientes comandos en tu terminal para crear el VHost y asignar permisos al usuario default:
```bash
docker exec -it rabbitmq-uci rabbitmqctl add_vhost /uci_app
docker exec -it rabbitmq-uci rabbitmqctl set_permissions -p /uci_app guest ".*" ".*" ".*"
```

### Paso C: Instalar las dependencias de Python
1. Abre una terminal en la carpeta del proyecto: `C:\Users\jeanc\Desktop\Arquitecutra de Software`
2. Instala la librería `pika`:
```bash
pip install -r requirements.txt
```

---

## 3. Guía de Ejecución

Para la demostración en vivo, levanta cada script en una terminal o pestaña separada en el siguiente orden:

### Terminal 1: Consumidor Médico (Monitoreo Clínico)
Este proceso escucha y procesa las lecturas de los pacientes. Confirma manualmente con ACK cada evento.
```bash
python consumidor_medico.py
```

### Terminal 2: Consumidor de Seguridad (Infraestructura y Bioseguridad)
Este proceso escucha alertas de seguridad física y avisos generales de bioseguridad de forma simultánea.
```bash
python consumidor_seguridad.py
```

### Terminal 3: Productor UCI (Simulador de Eventos)
Este script interactivo te permite enviar mensajes personalizados o simular un flujo continuo de telemetría médica en la UCI.
```bash
python productor_uci.py
```
- **Opción 1:** Envía telemetría de signos vitales (Topic). Puedes ingresar el número de cama y el valor.
- **Opción 2:** Envía alertas de severidad directas (Direct).
- **Opción 3:** Envía avisos generales a todo el hospital (Fanout).
- **Opción 4:** Inicia el bucle automático en tiempo real que emite datos cada 3 segundos.

---

## 4. Guía de Capturas para el Reporte de Laboratorio

Para armar el reporte detallado que solicita tu deber, abre el RabbitMQ Management Plugin (`http://localhost:15672`) mientras el simulador automático (Opción 4) está ejecutándose y realiza capturas de las siguientes secciones:

1. **Virtual Hosts (Aislamiento de Recursos)**:
   - *Ubicación:* Pestaña **Admin** -> **Virtual Hosts**.
   - *Qué mostrar:* La lista con el vhost `/uci_app` creado y activo.
2. **Exchanges (Configuración de Enrutadores)**:
   - *Ubicación:* Pestaña **Exchanges**. Selecciona el Virtual Host `/uci_app` en la lista desplegable superior.
   - *Qué mostrar:* Los tres exchanges creados: `uci.bioseguridad` (fanout), `uci.alertas` (direct), y `uci.monitoreo` (topic).
3. **Bindings (Configuración de Enlaces)**:
   - *Ubicación:* Pestaña **Exchanges** -> Haz clic sobre cada uno de los exchanges individuales.
   - *Qué mostrar:*
     - En `uci.monitoreo`, captura la tabla de bindings mostrando las claves `cama.*.ritmo_cardiaco`, `cama.*.oxigeno`, y `cama.*.temperatura` apuntando a `cola.monitoreo_medico`.
     - En `uci.alertas`, captura el enlace de `critical` apuntando a `cola.alertas_criticas` y de `warning`/`critical` apuntando a `cola.monitoreo_medico`.
     - En `uci.bioseguridad`, captura el enlace vacío (`""`) que direcciona directamente a `cola.notificaciones_generales`.
4. **Queues & Message Rates (Flujo de Mensajes)**:
   - *Ubicación:* Pestaña **Queues**.
   - *Qué mostrar:* Las tres colas `cola.monitoreo_medico`, `cola.alertas_criticas`, y `cola.notificaciones_generales` con sus estados (idle o active) y las gráficas/tasas de velocidad de entrega y confirmación de mensajes (`Publish`, `Deliver`, `Acknowledge`).
5. **Comprobación de ACK Manual (Tolerancia a fallos)**:
   - Detén el proceso de `consumidor_medico.py` con `Ctrl+C`.
   - Desde el `productor_uci.py`, envía 5 mensajes de tipo direct o topic.
   - Captura en el panel de RabbitMQ cómo el estado de esos mensajes en `cola.monitoreo_medico` cambia a **Ready** (en rojo/naranja), demostrando que no se pierden y quedan retenidos en el broker.
   - Inicia nuevamente `consumidor_medico.py` y captura cómo los procesa al instante y su estado en el panel vuelve a 0.

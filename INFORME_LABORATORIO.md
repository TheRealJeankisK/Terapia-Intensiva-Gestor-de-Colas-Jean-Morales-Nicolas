# INFORME DE LABORATORIO: DISEÑO Y ARQUITECTURA DE SOFTWARE

**Facultad:** Ingeniería y Ciencias Aplicadas (FICA)  
**Carrera:** Ingeniería de Software  
**Asignatura:** ISWZ2202 - Diseño y Arquitectura de Software  
**Tema:** Terapia Intensiva / Gestor de Colas Productor y Consumidor de Mensajería  
**Integrantes:**  
* Jean Gómez  
* [Apellido de Morales] [Nombre de Morales]  
* [Apellido de Nicolás] [Nombre de Nicolás]  

---

## 1. Tema del Proyecto
**Gestor de Alertas de Seguridad y Monitoreo Crítico en la Unidad de Cuidados Intensivos (UCI) mediante RabbitMQ y Python.**

---

## 2. Objetivos
* **Objetivo General:**  
  Diseñar e implementar un sistema distribuido de mensajería asíncrona basado en el patrón Productor-Consumidor utilizando RabbitMQ y Python para simular el monitoreo crítico y bioseguridad de una Unidad de Cuidados Intensivos (UCI).
* **Objetivos Específicos:**  
  1. Identificar y configurar tres tipos de enrutadores (*Exchanges*): Fanout, Direct y Topic dentro de un mismo Virtual Host de RabbitMQ.
  2. Implementar un productor simulador interactivo y automático de variables médicas estructuradas en formato JSON.
  3. Desarrollar dos consumidores concurrentes que gestionen y procesen datos clínicos y de seguridad física utilizando mecanismos de confirmación manual de recepción (*basic_ack*).

---

## 3. Arquitectura del Sistema (Patrón Productor-Consumidor)

El sistema implementa una arquitectura orientada a mensajes para garantizar el desacoplamiento físico y temporal de los componentes. Los datos médicos y alertas críticas fluyen desde el script productor a través de tres topologías de intercambio diferentes:

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

### Justificación del tipo de Exchanges elegidos:

1. **Exchange Fanout (`uci.bioseguridad`):**  
   Se utiliza para notificaciones masivas de bioseguridad e infraestructura del hospital. Al ser de tipo broadcast, difunde el mensaje sin importar claves de enrutamiento a todas las colas asociadas, lo cual es vital para emergencias generales (ej. "Corte eléctrico general").
2. **Exchange Direct (`uci.alertas`):**  
   Permite enrutar alertas con base en severidades exactas (`info`, `warning`, `critical`). Esto garantiza que los consumidores solo atiendan niveles específicos de urgencia sin procesamiento extra de strings.
3. **Exchange Topic (`uci.monitoreo`):**  
   Permite el filtrado dinámico de datos de telemetría de signos vitales. Estructurando la clave como `cama.<numero_cama>.<tipo_sensor>`, el consumidor médico puede monitorizar sensores clave de cualquier cama utilizando comodines (como `cama.*.ritmo_cardiaco`).

---

## 4. Configuración de RabbitMQ (Virtual Host, Exchanges, Colas y Bindings)

Toda la infraestructura está configurada dentro del Virtual Host `/uci_app` para garantizar el aislamiento de recursos.

### Tabla de Exchanges Declarados
| Nombre del Exchange | Tipo | Propósito en el Negocio |
| :--- | :--- | :--- |
| `uci.bioseguridad` | `fanout` | Difusión general de comunicados e infraestructura |
| `uci.alertas` | `direct` | Alertas de sistema clasificadas por gravedad exacta |
| `uci.monitoreo` | `topic` | Ruteo dinámico de datos biométricos de pacientes |

### Tabla de Colas y sus Enlaces (Bindings)
| Nombre de la Cola | Origen (Exchange) | Clave de Enlace (*Routing Key*) | Consumido por |
| :--- | :--- | :--- | :--- |
| `cola.monitoreo_medico` | `uci.monitoreo` | `cama.*.ritmo_cardiaco` | `consumidor_medico.py` |
| | `uci.monitoreo` | `cama.*.oxigeno` | |
| | `uci.monitoreo` | `cama.*.temperatura` | |
| | `uci.alertas` | `warning` | |
| | `uci.alertas` | `critical` | |
| `cola.alertas_criticas` | `uci.alertas` | `critical` | `consumidor_seguridad.py` |
| `cola.notificaciones_generales`| `uci.bioseguridad`| *(Ninguna / Broadcast)* | `consumidor_seguridad.py` |

---

## 5. Capturas de Pantalla de la Configuración en RabbitMQ

*Instrucciones: Ejecuta los scripts en tu terminal para inicializar automáticamente la topología de red. Luego, ingresa al panel de control web `http://localhost:15672` utilizando las credenciales `guest/guest` y toma las capturas correspondientes para colocarlas debajo de cada enunciado:*

### 5.1 Creación del Virtual Host `/uci_app`
*Toma una captura de la pestaña **Admin -> Virtual Hosts** donde se verifique que existe `/uci_app` con permisos para el usuario guest.*

> **[INSERTAR CAPTURA AQUÍ: Panel de Virtual Hosts]**

### 5.2 Configuración de los Exchanges
*Toma una captura de la lista de Exchanges dentro del Virtual Host `/uci_app`.*

> **[INSERTAR CAPTURA AQUÍ: Panel de Exchanges]**

### 5.3 Configuración de los Enlaces (Bindings)
*Entra en el detalle de cada Exchange y captura cómo están conectados a las colas:*
1. **Bindings de `uci.monitoreo` (Topic):**
   > **[INSERTAR CAPTURA AQUÍ: Enlaces de Telemetría Topic]**
2. **Bindings de `uci.alertas` (Direct):**
   > **[INSERTAR CAPTURA AQUÍ: Enlaces de Alertas Direct]**
3. **Bindings de `uci.bioseguridad` (Fanout):**
   > **[INSERTAR CAPTURA AQUÍ: Enlaces de Bioseguridad Fanout]**

### 5.4 Estado de las Colas y Tasas de Mensajes (Rates)
*Ejecuta el productor en modo de simulación automática (Opción 4). Captura la pestaña **Queues** donde se visualicen las tasas de mensajes por segundo (Publishing/Delivery rates) en tiempo real.*

> **[INSERTAR CAPTURA AQUÍ: Panel de Colas con flujos activos]**

---

## 6. Demostración y Pruebas de Tolerancia a Fallos (Garantía de Entrega)

Una de las características más críticas en sistemas médicos es el **mecanismo de confirmación manual de recepción (ACK)**. Para asegurar que ningún mensaje del paciente se pierda en caso de una desconexión o fallo del consumidor:

1. El código se configuró con `auto_ack=False`.
2. Se utiliza `basic_ack()` únicamente después de procesar y validar el mensaje de manera exitosa en el hilo consumidor.
3. Si un script consumidor se detiene de forma abrupta, el mensaje no confirmado se reencola automáticamente en el bróker (estado *Ready*).

### Captura de Prueba de Tolerancia a Fallos:
*Procedimiento para la captura:*
1. Apaga el script `consumidor_medico.py` presionando `Ctrl+C`.
2. Ejecuta el `productor_uci.py` y envía 3 lecturas médicas.
3. Toma una captura en el panel web de RabbitMQ de la sección **Queues -> `cola.monitoreo_medico`** mostrando que los mensajes quedan acumulados en estado **Ready** (en lugar de perderse).
4. Vuelve a encender `consumidor_medico.py` y captura cómo el consumidor lee inmediatamente los mensajes rezagados y el contador de RabbitMQ vuelve a 0.

> **[INSERTAR CAPTURA AQUÍ: Mensajes acumulados en estado Ready]**

---

## 7. Conclusiones y Recomendaciones

* **Conclusiones:**
  * Se logró implementar satisfactoriamente el patrón de diseño Productor-Consumidor distribuyendo tareas de forma asíncrona.
  * La separación lógica mediante Exchanges en RabbitMQ permitió que un solo emisor pudiera distribuir información dirigida selectivamente a departamentos médicos y de seguridad sin sobrecargar la red ni requerir lógica compleja de filtrado en el cliente.
  * El uso de la propiedad `durable` en colas y el envío manual de confirmaciones (`basic_ack`) demostró ser un pilar de tolerancia a fallos indispensable en escenarios de infraestructura crítica como la UCI de un hospital.
* **Recomendaciones:**
  * Para despliegues en producción real se sugiere activar clústeres redundantes de RabbitMQ y habilitar cifrado TLS para proteger los datos médicos y la telemetría de los pacientes.
  * Implementar una cola secundaria de reintentos (*Dead Letter Exchange*) para almacenar payloads JSON corruptos y evitar el bloqueo permanente de los consumidores (*poison messages*).

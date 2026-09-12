<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="brands_assets/dark_logo.png">
  <img src="brands_assets/logo.png" alt="Scribe" width="300">
</picture>

### El historial de Home Assistant en TimescaleDB

Cada estado y cada evento, mediante `asyncpg` — sin bloquear el bucle de eventos.

[![Release](https://img.shields.io/github/v/release/jonathan-gtd/scribe?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases/latest) [![Downloads](https://img.shields.io/github/downloads/jonathan-gtd/scribe/total?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases) [![Tests](https://img.shields.io/github/actions/workflow/status/jonathan-gtd/scribe/tests.yaml?branch=master&label=tests)](https://github.com/jonathan-gtd/scribe/actions/workflows/tests.yaml) [![License](https://img.shields.io/github/license/jonathan-gtd/scribe?color=lightgrey)](LICENSE)

[![lang en](https://img.shields.io/badge/lang-en-lightgrey)](README.md) [![lang fr](https://img.shields.io/badge/lang-fr-lightgrey)](README.fr.md) [![lang es](https://img.shields.io/badge/lang-es-41BDF5)](README.es.md) [![lang de](https://img.shields.io/badge/lang-de-lightgrey)](README.de.md) [![lang nl](https://img.shields.io/badge/lang-nl-lightgrey)](README.nl.md)

</div>

---

El recorder de Home Assistant guarda unas semanas de historial en SQLite y se ralentiza a medida que crece. Scribe escribe los mismos estados y eventos en **TimescaleDB**, donde los años siguen siendo rápidos y ocupan una fracción del espacio.

- 🚀 **Asíncrono de extremo a extremo** — `asyncpg` y `COPY` por lotes: el registro nunca bloquea Home Assistant.
- 🗜️ **Comprimido automáticamente** — el historial antiguo se divide en chunks y se comprime, normalmente 10× más pequeño.
- 🛟 **Nada se pierde** — una base de datos caída se almacena en búfer y se escribe cuando vuelve.
- 🧩 **Con su contexto** — entidades, dispositivos, áreas, usuarios e integraciones, no solo valores.
- 🩺 **Avisa cuando algo va mal**, en Reparaciones en lugar de en un registro que nadie lee.

---

## Instalación

**1. Una base de datos TimescaleDB.** La extensión es obligatoria — vea *Instalar TimescaleDB* más abajo.

**2. Scribe, mediante HACS:**

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=jonathan-gtd&repository=scribe&category=integration)

*O a mano:* copie `custom_components/scribe` en su carpeta `custom_components`. En ambos casos, reinicie Home Assistant.

**3. La URL de la base de datos.** Vaya a **Ajustes → Dispositivos y servicios → Añadir integración**, busque **Scribe** y péguela:

[![Open your Home Assistant instance and start setting up a new integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=scribe)

```
postgresql://scribe:password@192.168.1.10:5432/scribe
```

*O en `configuration.yaml`*, si prefiere mantener su configuración en archivos:

```yaml
scribe:
  db_url: "postgresql://scribe:password@192.168.1.10:5432/scribe"
```

Listo — los estados se registran, se dividen en chunks y se comprimen, con su contexto de entidad, dispositivo y área. Todo lo que sigue es opcional.

---

<details>
<summary><b>🧩 Scribe Card — gráficos en su panel</b></summary>
<br>

**[Scribe Card](https://github.com/jonathan-gtd/scribe-card)** muestra cualquier consulta de su historial en un panel. Dibujada con Apache ECharts —la biblioteca que usan los propios gráficos de historial de Home Assistant— y configurada en un formulario, donde el tipo de gráfico, la unidad y los ejes se eligen entre las columnas que devuelve su consulta.

Usa el servicio `scribe.query`, así que **no hay una segunda conexión a la base de datos que configurar ni una contraseña en su panel**. Instálela mediante HACS como repositorio personalizado, categoría *Panel*.

</details>

<details>
<summary><b>🗄️ Instalar TimescaleDB</b></summary>
<br>

### Preparar la base de datos

Necesitas una instancia de TimescaleDB en funcionamiento. Recomiendo PostgreSQL 17 o 18.

> **❗ Importante** — **La extensión TimescaleDB es obligatoria.** La división en chunks, la
> compresión, la retención y los sensores de tamaño son la razón de ser de
> Scribe, y ninguno existe en PostgreSQL a secas. Una instalación nueva se
> rechaza si falta la extensión, aunque Scribe la activa por ti cuando el
> servidor la tiene disponible y tu usuario de la base de datos posee `CREATE`
> sobre ella, que es lo que concede la preparación de abajo. Las instalaciones
> que ya funcionan sin ella siguen registrando y se les indica lo que les falta
> mediante un problema en Reparaciones.

#### Opción A: Home Assistant OS (complemento)

Si usas Home Assistant OS, recomiendo el [complemento TimescaleDB](https://github.com/expaso/hassos-addon-timescaledb).

[![Abre tu instancia de Home Assistant y muestra el diálogo de añadir repositorio de complementos con una URL ya rellenada.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Fexpaso%2Fhassos-addon-timescaledb)

#### Opción B: Docker (manual)

```bash
# Alta disponibilidad (recomendado)
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg18

# Estándar
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:pg18
```

Crea la base de datos y el usuario:

```sql
CREATE DATABASE scribe;
CREATE USER scribe WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE scribe TO scribe;

\c scribe
CREATE EXTENSION IF NOT EXISTS timescaledb;
GRANT ALL ON SCHEMA public TO scribe;
```

</details>

<details>
<summary><b>⚙️ Todas las opciones, con sus valores por defecto</b></summary>
<br>

### Configuración completa (valores por defecto)

```yaml
scribe:
  # La única opción obligatoria.
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe

  # Todo lo demás es opcional. Estos son los valores por defecto.

  # Dónde registra
  db_schema: ""                 # vacío = el esquema de la conexión, normalmente public
  db_ssl: false                 # TLS hacia la base de datos
  ssl_root_cert: ""             # certificado CA; solo se lee si db_ssl es true
  ssl_cert_file: ""             # certificado de cliente, para TLS mutuo
  ssl_key_file: ""              # su clave privada

  # Qué registra
  record_states: true           # los cambios de estado
  record_events: false          # los eventos de Home Assistant (automatizaciones, scripts…)
  include_domains: []           # vacío = todos los dominios
  include_entities: []          # vacío = todas las entidades
  include_entity_globs: []      # p. ej. sensor.tiempo_*
  exclude_domains: []           # se aplica después de las listas de inclusión
  exclude_entities: []
  exclude_entity_globs: []
  exclude_attributes: []        # atributos que se quitan de la columna attributes
  include_events: []            # vacío = todos los tipos de evento
  exclude_events: []            # se aplica después de include_events

  # Cuánto tiempo lo guarda
  chunk_time_interval: "7 days" # tiempo que abarca un chunk
  compress_after: "7 days"      # los chunks más antiguos se comprimen
  retention_states: ""          # vacío = para siempre; si no, BORRA los estados anteriores
  retention_events: ""          # vacío = para siempre; si no, BORRA los eventos anteriores
  enable_rollups: false         # resúmenes por hora y por día de los estados numéricos

  # Cómo escribe
  batch_size: 500               # filas en el búfer antes de una escritura
  flush_interval: 30            # segundos antes de escribir un lote incompleto
  max_queue_size: 10000         # filas en memoria antes de descartar las nuevas
  buffer_on_failure: true       # seguir almacenando mientras la base no responda

  # Lo que scribe.query puede costar
  query_timeout: 60             # segundos que puede durar una consulta
  query_max_rows: 20000         # filas que puede devolver antes de ser rechazada

  # Sensores sobre el propio Scribe
  enable_stats_io: false        # contadores del writer, leídos en memoria
  enable_stats_chunk: false     # número de chunks, una consulta por actualización
  enable_stats_size: false      # tamaños en disco, una consulta por actualización
  stats_io_interval: 60         # segundos entre dos valores de escritura
  stats_chunk_interval: 60      # minutos entre dos consultas de chunks
  stats_size_interval: 60       # minutos entre dos consultas de tamaño

  # Tablas de contexto, sincronizadas con los registros de Home Assistant
  enable_table_areas: true
  enable_table_devices: true
  enable_table_integrations: true
  enable_table_users: true
```

</details>

<details>
<summary><b>📋 Referencia de parámetros</b></summary>
<br>

| Parámetro | Descripción |
| :--- | :--- |
| `db_url` | **Obligatorio.** Cadena de conexión a tu base de datos TimescaleDB. |
| `db_ssl` | Activar SSL/TLS para la conexión a la base de datos. |
| `ssl_root_cert` | Ruta al archivo de la CA (p. ej. `/ssl/ca.crt`). Una ruta relativa se resuelve desde el directorio de configuración de Home Assistant. |
| `ssl_cert_file` | Ruta al certificado de cliente, para TLS mutuo. |
| `ssl_key_file` | Ruta a la clave privada del cliente, para TLS mutuo. |
| `db_schema` | Esquema PostgreSQL en el que registrar. Vacío (predeterminado): el de la conexión, normalmente `public`. |
| `chunk_time_interval` | Cuánto tiempo abarca cada chunk de la tabla. Ver *Ajuste del almacenamiento* más abajo. |
| `compress_after` | Los chunks más antiguos que este intervalo se comprimen. Ver *Ajuste del almacenamiento* más abajo. |
| `retention_states` | **Elimina** el historial de estados más antiguo que este intervalo (p. ej. `"365 days"`). Vacío (por defecto) conserva todo. Ver *Retención* más abajo. |
| `retention_events` | **Elimina** el historial de eventos más antiguo que este intervalo. Vacío (por defecto) conserva todo. Ver *Retención* más abajo. |
| `record_states` | Registrar o no los cambios de estado. |
| `record_events` | Registrar o no los eventos. |
| `batch_size` | Número de elementos que se acumulan antes de escribir en la base de datos. |
| `flush_interval` | Segundos antes de que un lote incompleto se escriba igualmente (por defecto `30`). Cada volcado es una transacción: un intervalo corto escribe unas pocas filas cada vez en lugar de agruparlas, y uno largo solo arriesga el último intervalo de historial, y solo si Home Assistant muere. |
| `max_queue_size` | Número máximo de elementos en memoria antes de descartar los nuevos. |
| `query_timeout` | Segundos que una llamada a `scribe.query` puede ejecutarse antes de que la base la detenga (por defecto `60`). |
| `query_max_rows` | Filas que una llamada a `scribe.query` puede devolver antes de ser rechazada (por defecto `20000`). |
| `buffer_on_failure` | Si es verdadero, mantiene los datos en memoria cuando la base de datos no responde (hasta `max_queue_size`). |
| `enable_stats_io` | Activar los sensores de rendimiento del escritor en tiempo real (sin consultas a la base). |
| `enable_stats_chunk` | Activar los sensores de número de chunks (consultan la base). |
| `enable_stats_size` | Activar los sensores de tamaño de almacenamiento (consultan la base). |
| `stats_io_interval` | Segundos entre dos valores de los sensores de E/S (por defecto `60`). Cada cambio es una fila que Scribe registra sobre sí mismo. |
| `stats_chunk_interval` | Intervalo (en minutos) de actualización de las estadísticas de chunks. |
| `stats_size_interval` | Intervalo (en minutos) de actualización de las estadísticas de tamaño. |
| `include_domains` | Lista de dominios a incluir. |
| `include_entities` | Lista de entidades concretas a incluir. |
| `include_entity_globs` | Lista de patrones de entidad a incluir (p. ej. `sensor.weather_*`). |
| `exclude_domains` | Lista de dominios a excluir. |
| `exclude_entities` | Lista de entidades concretas a excluir. |
| `exclude_entity_globs` | Lista de patrones de entidad a excluir (p. ej. `switch.kitchen_*`). |
| `exclude_attributes` | Lista de atributos a excluir de la columna `attributes`. |
| `include_events` | Lista de tipos de evento a registrar. Déjala vacía para registrarlos todos. |
| `exclude_events` | Lista de tipos de evento que nunca se registran (se aplica después de `include_events`). |
| `enable_table_areas` | Activar la creación y sincronización de la tabla `areas`. |
| `enable_table_devices` | Activar la creación y sincronización de la tabla `devices`. |
| `enable_table_integrations` | Activar la creación y sincronización de la tabla `integrations`. |
| `enable_table_users` | Activar la creación y sincronización de la tabla `users`. |
| `enable_rollups` | Mantener resúmenes por hora y por día precalculados de los estados (`states_hourly`, `states_daily`). Desactivado por defecto. |

</details>

<details>
<summary><b>🗜️ Ajuste del almacenamiento — chunks y compresión</b></summary>
<br>

Scribe guarda el historial en **hypertables** de TimescaleDB: una tabla que se
usa y se consulta como cualquier otra, pero que está físicamente dividida en
**chunks**, cada uno cubriendo un tramo de tiempo. Casi todo lo relativo al
espacio en disco y a la velocidad de las consultas viene de esa división: una
consulta sobre la semana pasada solo lee los chunks que la solapan, la
compresión trabaja chunk a chunk, y la *retención* más abajo elimina chunks
enteros en lugar de filas sueltas.

Lo controlan dos ajustes, tanto en YAML como en la interfaz, en
**Configurar → Avanzado (TimescaleDB y SSL)**:

### `chunk_time_interval` (por defecto `7 days`)

Cuánto tiempo abarca un chunk.

- **Chunks más pequeños** (p. ej. `1 day`): más archivos y más pequeños — una
  retención más fina, y las consultas sobre ventanas recientes tocan menos
  datos. Pasado cierto punto, una consulta de varios meses tiene que abrir
  cientos de chunks.
- **Chunks más grandes** (p. ej. `30 days`): menos archivos y más grandes —
  mejor para consultas históricas largas, peor para la memoria. La propia
  recomendación de TimescaleDB es que los chunks en los que escribes quepan
  holgadamente en memoria junto con sus índices, así que un chunk sobredimensionado
  en una máquina pequeña perjudica las escrituras.

El valor por defecto es adecuado para una instancia típica de Home Assistant.
Plantéate `1 day` si registras miles de entidades, y solo entonces.

> **El cambio solo afecta a los chunks nuevos.** Los ya escritos conservan el
> tramo con el que se crearon, y nada se reescribe ni se mueve: simplemente
> tendrás una mezcla de tramos antiguos y nuevos, algo que TimescaleDB gestiona
> de forma nativa.

### `compress_after` (por defecto `7 days`)

Qué antigüedad debe tener un chunk para que TimescaleDB lo comprima. Con este
tipo de datos (muchos `entity_id` repetidos y valores que cambian despacio) la
compresión suele reducir mucho el tamaño, por eso está activada por defecto.

Los chunks comprimidos siguen siendo perfectamente consultables — a la vista
`states` le da igual. Escribir *dentro* de uno sí es más lento, y por eso la
compresión solo entra en juego cuando el chunk ya es lo bastante antiguo como
para darse por terminado. Mantén `compress_after` holgadamente por encima de la
antigüedad de los datos que aún escribes: los estados que llegan desordenados
(un relleno retroactivo, un script de migración) aterrizan en chunks antiguos.

> **El cambio surte efecto en el siguiente reinicio**, y los chunks ya
> comprimidos siguen comprimidos: el ajuste solo decide cuándo se comprimirán
> los *siguientes*.

### Cómo encajan los tres ajustes

| Ajuste | Qué hace | Reversible |
| :--- | :--- | :--- |
| `chunk_time_interval` | Cuánto tiempo abarca un chunk | Sí — solo chunks futuros |
| `compress_after` | Cuándo se comprime un chunk | Sí |
| `retention_states` / `retention_events` | Cuándo se **elimina** un chunk | **No** |

Se aplican en ese orden al mismo chunk a lo largo de su vida: escrito →
comprimido → eliminado. Dos consecuencias que conviene conocer:

- Si `compress_after` es mayor que tu retención, los chunks se eliminan antes de
  haberse comprimido nunca, y la compresión no hace nada.
- La retención elimina chunks enteros, así que tu ventana real es el intervalo
  que fijas **más** hasta un `chunk_time_interval`. Chunks más pequeños la
  ajustan mejor.

Si los sensores de tamaño y de chunks están activados (`enable_stats_size`,
`enable_stats_chunk`), informan exactamente de lo que producen estos ajustes:
número de chunks, tamaños comprimido y sin comprimir, y ratio de compresión.

</details>

<details>
<summary><b>🧹 Retención — eliminar el historial antiguo automáticamente</b></summary>
<br>

Por defecto, Scribe lo conserva todo, para siempre. Si solo quieres almacenar
una ventana acotada —porque agregas el historial en bruto en otro sitio, o
simplemente para limitar el disco—, fija un intervalo de retención y TimescaleDB
eliminará los chunks más antiguos:

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  retention_states: "365 days"
  retention_events: "30 days"
```

Ambos están también en la interfaz, en **Configurar → Avanzado (TimescaleDB y SSL)**.

> **⚠️ Atención** — La retención **elimina datos de forma permanente**. No hay deshacer ni
> papelera: en cuanto un chunk queda fuera de la ventana se elimina, y solo una
> copia de seguridad puede recuperarlo. Estados y eventos se configuran por
> separado, así puedes caducar eventos ruidosos conservando el historial de
> estados.

Conviene saber:

- **Ningún ajuste significa siempre «conservar para siempre».** Vaciar el campo
  en la interfaz y borrar la línea de `configuration.yaml` eliminan ambos la
  política: a un valor importado en su día desde el YAML nunca se le permite
  sobrevivir a la línea que lo definió.
- **Scribe es dueño de la política de retención en sus propias tablas.** Vaciar
  el campo la elimina —incluida una que hayas creado a mano con
  `add_retention_policy()`—, que es la única forma de que vaciar el ajuste en la
  interfaz detenga realmente las eliminaciones.
- **Empieza de inmediato.** TimescaleDB ejecuta la política a los pocos segundos
  de crearla, no en el siguiente intervalo diario: todo lo que queda fuera de la
  ventana desaparece en la primera ejecución, justo tras el reinicio que la
  activó.
- **La eliminación es por chunk, no por fila.** Un chunk solo se elimina cuando
  *todas* sus filas son más antiguas que el intervalo, así que con el
  `chunk_time_interval` por defecto de 7 días conservas hasta una semana más de
  lo que pediste. Eso es lo que hace la retención casi gratuita: elimina
  archivos en lugar de borrar filas.
- **Solo se elimina el historial.** La tabla `entities` y las demás tablas de
  metadatos no se tocan, así que una entidad cuyo historial ha caducado por
  completo sigue resolviéndose.
- **TimescaleDB es imprescindible**: es la extensión que ejecuta la política. En
  PostgreSQL sin ella, fijar un intervalo de retención genera un problema en
  Reparaciones en lugar de no hacer nada en silencio.
- Los valores aceptados son intervalos simples: `30 days`, `6 months`,
  `1 year`. Cualquier otra cosa se rechaza con un error en vez de enviarse a la
  base de datos.

</details>

<details>
<summary><b>📈 Resúmenes — agregados por hora y por día</b></summary>
<br>

Un año de un sensor que informa cada 30 segundos son cerca de un millón de filas. Un gráfico de ese año las lee todas, cada vez que se dibuja.

Con `enable_rollups: true`, TimescaleDB mantiene al día dos resúmenes de sus estados a medida que se escriben —por hora y por día— y un gráfico de varios años lee miles de filas en lugar de millones.

```yaml
scribe:
  enable_rollups: true
```

También está en la interfaz, en **Configurar → Tablas de metadatos**. Esto añade dos vistas:

| Vista | Una fila por | Columnas |
| --- | --- | --- |
| `states_hourly` | entidad y hora | `entity_id`, `bucket`, `value_avg`, `value_min`, `value_max`, `samples` |
| `states_daily` | entidad y día | las mismas |

```sql
SELECT bucket, value_avg, value_min, value_max
FROM states_daily
WHERE entity_id = 'sensor.temperatura_exterior'
  AND bucket > now() - interval '2 years'
ORDER BY bucket;
```

Solo se resumen los estados numéricos —la media de `on` y `off` no significa nada— así que `value_avg`, `value_min` y `value_max` quedan vacíos para el resto, mientras que `samples` cuenta todos los estados del bucket.

**Son datos derivados.** No se duplica nada que vaya a echar de menos: desactivar la opción elimina ambas vistas, reactivarla las reconstruye a partir del historial, y sus estados no se tocan en ningún caso. TimescaleDB las refresca por sí mismo —la de por hora cada 30 minutos, la diaria cada hora— y cada pasada mira lo bastante atrás (3 días, 30 días) como para que un lote escrito tarde acabe en ellas. Scribe solo las crea.

</details>

<details>
<summary><b>🗃️ Registrar en un esquema de PostgreSQL concreto</b></summary>
<br>

Por defecto Scribe registra en el esquema al que ya apunta su conexión —normalmente `public`—. Indique `db_schema` y creará ese esquema y pondrá todo en él: sus tablas, sus vistas, sus hypertables y sus políticas.

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  db_schema: scribe
```

También está en la interfaz, en **Configurar → Avanzado (TimescaleDB y SSL)**.

Es lo que quiere cuando Scribe comparte una base con otra cosa: las tablas de otra integración, sus propias copias del historial, o un segundo Home Assistant registrando en el mismo servidor. Los esquemas son independientes —tablas, hypertables, retención y compresión separadas— y nada de lo que Scribe hace en uno alcanza al otro.

- **Solo van allí los datos nuevos.** Indicar `db_schema` no mueve el historial ya registrado. Muévalo usted antes de reiniciar (`ALTER TABLE public.states_raw SET SCHEMA scribe;`), o consulte el esquema antiguo directamente.
- **Scribe crea el esquema si puede**, lo que requiere `CREATE` sobre la base. Uno creado a mano también sirve, con `USAGE` y `CREATE` sobre él.
- **Un esquema inalcanzable detiene el registro.** PostgreSQL pasa a la siguiente entrada del search path en lugar de fallar, así que una errata llenaría `public` mientras la interfaz muestra otra cosa. Scribe comprueba dónde ha aterrizado y no registra nada antes que registrar en el sitio equivocado, con un problema en Reparaciones que dice qué conceder.
- **Sus consultas no cambian.** Scribe pone el esquema primero en el `search_path` de la conexión, así que `SELECT * FROM states` sigue funcionando a través de `scribe.query`. Desde Grafana o psql, califique el nombre (`scribe.states`) o defina su propio `search_path`. `public` sigue en la ruta: ahí viven las funciones de TimescaleDB.
- Los valores aceptados son identificadores simples: letras, dígitos y guiones bajos, sin empezar por un dígito. Vacío mantiene el esquema de la conexión, incluido uno que haya fijado usted con `?options=-csearch_path%3Dmiesquema` en la URL.

**Las tablas en sí** —cada columna, cómo se relacionan y recetas de consulta para Grafana y `scribe.query`— están documentadas en [`docs/data-structure.md`](docs/data-structure.md).

</details>

<details>
<summary><b>🛠️ Servicios — flush, query, purge</b></summary>
<br>

### `scribe.flush`
Fuerza el volcado inmediato a la base de datos de los datos en búfer.

```yaml
service: scribe.flush
```

### `scribe.query`
Ejecuta una consulta SQL de solo lectura contra la base de datos TimescaleDB.

**Parámetros:**
- `sql` (obligatorio): la consulta SQL a ejecutar. Debe ser una sentencia `SELECT`.

**Devuelve:**
Una lista de filas, donde cada fila es un diccionario de nombres de columna y valores.

**Ejemplo:**
```yaml
service: scribe.query
data:
  sql: "SELECT * FROM states ORDER BY time DESC LIMIT 5"
response_variable: query_result
```

### `scribe.purge`
Elimina el historial registrado. **Esto no se puede deshacer.**

**Parámetros** (se requiere al menos uno de los dos primeros):
- `entity_id`: las entidades a purgar. Sin `keep_days`, se elimina todo su historial *y* su fila en la tabla `entities`; volver a registrarlas empieza de cero.
- `keep_days`: elimina todo lo más antiguo que este número de días.
- `events` (por defecto `false`): elimina también los eventos más antiguos que `keep_days`. Sin esa duración, se ignora.

**Devuelve:** cuántos estados, eventos y filas de entidades se eliminaron.

**Ejemplos:**
```yaml
# Retirar por completo una entidad de la base de datos
action: scribe.purge
data:
  entity_id: sensor.sensor_que_ya_no_quiero
```

```yaml
# Recortar todo lo que tenga más de dos años, eventos incluidos
action: scribe.purge
data:
  keep_days: 730
  events: true
response_variable: purged
```

El historial comprimido también se purga: TimescaleDB se encarga y los chunks siguen comprimidos. Para una ventana deslizante que se aplique continuamente, use los ajustes de *Retención* más abajo: una purga es puntual.

</details>

<details>
<summary><b>📊 Sensores de estadísticas</b></summary>
<br>

Activa los sensores estableciendo sus opciones en la configuración.

### Estadísticas de escritura (`enable_stats_io: true`)

Métricas en tiempo real del escritor (sin consultas a la base de datos).

| Sensor | Descripción |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_states_written` | Número total de cambios de estado escritos en la base. |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_events_written` | Número total de eventos escritos en la base. |
| <img src="https://api.iconify.design/mdi:buffer.svg?color=%232196F3" width="15" /> `sensor.scribe_buffer_size` | Elementos que esperan actualmente en el búfer de memoria. |
| <img src="https://api.iconify.design/mdi:timer-sand.svg?color=%232196F3" width="15" /> `sensor.scribe_last_write_duration` | Tiempo (en ms) de la última escritura en la base. |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_states_rate` | Ritmo de estados escritos (por minuto). |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_events_rate` | Ritmo de eventos escritos (por minuto). |

### Estadísticas de chunks (`enable_stats_chunk: true`)

Número de chunks (actualizado cada `stats_chunk_interval` minutos).

| Sensor | Descripción |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_chunks` | Número total de chunks de la tabla de estados. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_chunks` | Número de chunks ya comprimidos. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_chunks` | Número de chunks pendientes de comprimir. |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_chunks` | Número total de chunks de la tabla de eventos. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_chunks` | Número de chunks de eventos comprimidos. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_chunks` | Número de chunks de eventos sin comprimir. |

### Estadísticas de tamaño (`enable_stats_size: true`)

Espacio ocupado en bytes (actualizado cada `stats_size_interval` minutos).

| Sensor | Descripción |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_size` | Tamaño total en disco (datos comprimidos + chunks recientes + índices). |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_states_original_size` | **Tamaño teórico** si los datos no estuvieran comprimidos (p. ej. 11 GB). |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_size` | Tamaño físico de los chunks de datos comprimidos. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_size` | Tamaño de los datos recientes aún sin comprimir (o índices pendientes). |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compression_ratio` | Ratio de compresión de los estados (%). |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_size` | Tamaño total en disco de la tabla de eventos. |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_events_original_size` | Tamaño teórico de los eventos antes de comprimir. |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_size` | Tamaño de los datos de eventos comprimidos. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_size` | Tamaño de los datos de eventos sin comprimir. |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compression_ratio` | Ratio de compresión de los eventos (%). |

</details>

<details>
<summary><b>🖼️ Panel</b></summary>
<br>

En este repositorio hay una disposición Lovelace lista para usar con todos los
sensores útiles de Scribe (estadísticas de la base, ratios de compresión,
rendimiento de escritura), en dos variantes:

| Archivo | Qué es | Dónde pegarlo |
| --- | --- | --- |
| [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml) | Una **sola tarjeta** (`type: vertical-stack`) | El editor YAML de tarjeta («Añadir tarjeta» → «Manual») |
| [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml) | Una **vista completa** (`title` / `icon` / `cards`) | El editor YAML de vista |

> ⚠️ No son intercambiables. Pegar el archivo de *vista* en un editor de
> *tarjeta* falla con **«No card type configured»**, porque la configuración de
> una tarjeta debe empezar por una clave `type:`.

**Opción A — añadirla como tarjeta (lo más fácil, funciona en cualquier tipo de vista):**

1.  Abre tu panel y pulsa «Editar panel» (icono del lápiz).
2.  Pulsa **+ Añadir tarjeta** y baja hasta el final del selector para elegir **Manual**.
3.  Copia el contenido de [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml), sustituye todo lo que haya en el editor y pulsa **Guardar**.

**Opción B — añadirla como vista dedicada:**

1.  Abre tu panel y pulsa «Editar panel» (icono del lápiz).
2.  Pulsa el botón **+** *en la barra de pestañas superior* (junto a los nombres de tus vistas) para añadir una vista, no el botón «Añadir tarjeta».
3.  En el diálogo de la vista, abre el menú ⋮ (o el botón «Mostrar editor de código») y elige **Editar en YAML**.
4.  Copia el contenido de [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml), sustituye todo lo que haya en el editor y pulsa **Guardar**.

</details>

<details>
<summary><b>📦 Migrar desde InfluxDB, LTSS, el recorder o Scribe 2.x</b></summary>
<br>

### Actualizar desde Scribe 2.x

Scribe 3.0 sustituyó la tabla `states` por `states_raw` más una vista de
compatibilidad, y dio a `entities` una clave primaria numérica. La conversión de
una base antigua la hacían las versiones 3.x y se **eliminó en la 3.9**.

Si tu base de datos todavía tiene una *tabla* `states` (en lugar de una vista),
una tabla `states_legacy`, o una tabla `entities` sin columna `id`, Scribe se
detiene al arrancar, no registra nada y genera un problema en Reparaciones, sin
renombrar, crear ni eliminar nada. Instala **Scribe 3.8**, deja Home Assistant
en marcha hasta que los registros indiquen que la migración ha terminado (unos
quince minutos en una base grande) y actualiza de nuevo.

Las instalaciones nuevas y cualquier base creada por una versión 3.x no se ven
afectadas.

### Importar datos desde otras fuentes

`migration/` contiene tres scripts que copian un historial a Scribe desde otro sitio. Se ejecutan a mano, una vez, desde una máquina que alcance ambas bases —y Scribe debe haber arrancado al menos una vez, para que sus tablas existan—.

```bash
cd migration
pip install psycopg2-binary python-dotenv   # más influxdb-client para InfluxDB
cp .env.example .env && nano .env
python3 <script>.py
```

| Fuente | Script | Qué rellenar |
| --- | --- | --- |
| InfluxDB | `influx2scribe.py` | `INFLUX_*` |
| LTSS | `ltss2scribe.py` | `LTSS_*` |
| Recorder de Home Assistant | `recorder2scribe.py` | `RECORDER_*`, con `RECORDER_TYPE` en `postgres` o `sqlite` (SQLite solo necesita `RECORDER_DB_PATH`) |

Cada ejecución necesita además `SCRIBE_*` —el destino— y los ajustes de migración: `MIGRATION_START_TIME`, `MIGRATION_END_TIME`, `CHUNK_SIZE` (horas por lote) y `PURGE_DESTINATION`, que **borra el historial del destino antes de importar**. Déjelo en `False` salvo que sea lo que quiere.

Cada script comprueba el esquema de destino antes de escribir nada, y se detiene con una explicación en lugar de un muro de errores fila a fila si Scribe nunca lo inicializó.

</details>

<details>
<summary><b>🩺 Solución de problemas</b></summary>
<br>

### Lo primero que hay que mirar

Dos sitios responden a «¿por qué no se registra nada?» sin leer una sola línea de registro:

- **Ajustes → Dispositivos y servicios → Scribe → ⋮ → Descargar diagnósticos** informa de
  lo que está haciendo realmente el escritor: conectado o no, si se encontró TimescaleDB,
  cuántos elementos esperan en el búfer y cuántos se descartaron, los fallos de escritura
  consecutivos, y los ajustes de almacenamiento y retención vigentes. La URL de la base de
  datos nunca aparece, y a los errores del controlador se les quita cualquier cadena de conexión.
- **Ajustes → Sistema → Reparaciones** lista los problemas de abajo, y
  **Ajustes → Sistema → Estado del sistema** muestra a qué base apunta Scribe y si está
  conectado en este momento.

### Reparaciones

Scribe informa de los problemas que no puede resolver por sí mismo en
**Ajustes → Sistema → Reparaciones**, para que no tengas que vigilar los
registros. Cada uno desaparece solo en cuanto se corrige la causa.

| Reparación | Qué significa |
| --- | --- |
| No puede acceder a su base de datos | La conexión falló. Scribe sigue almacenando en búfer y reintenta en segundo plano, así que el historial registrado durante el corte se escribe en cuanto la base vuelve. Comprueba que el servidor esté activo y que la URL y las credenciales sean correctas. |
| No puede escribir en su base de datos | Varias escrituras consecutivas fallaron. Los datos se mantienen en memoria y se escriben al recuperarse, salvo que Home Assistant se reinicie antes. |
| El búfer está lleno | Las escrituras fallaron el tiempo suficiente para saturar el búfer; los registros más antiguos se están descartando. Arregla la base de datos o sube `max_queue_size`. |
| Descartando registros | Una escritura falló con el búfer desactivado, así que los registros se perdieron de inmediato. Activa el búfer para sobrevivir a cortes breves. |
| No pudo crear sus tablas | Scribe llegó a la base de datos pero no pudo construir su esquema, casi siempre un problema de permisos. En una base nueva no se registra nada en absoluto. |
| No puede alcanzar el esquema indicado | El esquema de `db_schema` no existe y no se pudo crear, o el usuario de la base de datos no tiene permisos sobre él. No se registra nada, en lugar de llenar `public` en silencio. |
| No pudo crear la vista `states` | El historial se registra, pero falta la vista por la que pasan todas las consultas: el historial parece vacío aunque no se ha perdido nada. |
| `states_raw` / `events` no es una hypertable | TimescaleDB está instalado pero la tabla nunca se convirtió (algo habitual cuando la extensión se añade *después* de llenar las tablas). Los chunks, la compresión y la retención no hacen nada. |
| `states_raw` / `events` nunca se comprime | La tabla sí es una hypertable pero no tiene política de compresión, así que mantiene su tamaño sin comprimir. |
| TLS no aplicado por completo | Scribe conecta por TLS, pero un certificado configurado no pudo aplicarse — casi siempre un certificado de cliente: se autentica como un cliente cualquiera en lugar del que aprovisionaste. |
| TimescaleDB no está instalado | El historial se registra, pero la división en chunks y la compresión no están disponibles: la base crece mucho más rápido y los sensores de tamaño se quedan vacíos. |
| La base es anterior a la versión 3.0 | La base todavía usa el esquema anterior a 3.0, que esta versión no sabe convertir. No se registra nada y no se ha modificado nada: instala Scribe 3.8 para convertirla y vuelve a actualizar. |
| No se pudo aplicar la política de retención | Pediste eliminar los datos anteriores a un intervalo y la política no pudo crearse. No se ha eliminado nada y nada se está eliminando: la tabla sigue creciendo. |
| El cambio de nombre de entidad no se aplicó | Un cambio de nombre chocó con una fila ya existente en la base. El historial de la entidad queda repartido entre dos identificadores. |

### Consumo de memoria elevado
- Reduce `max_queue_size`
- Reduce `flush_interval` para vaciar el búfer más a menudo
- Vigila `sensor.scribe_buffer_size`

### Ajuste del rendimiento

Si la vista `states` va lenta (varios segundos por consulta), suele deberse a
que el planificador de PostgreSQL elige un **Hash Join** en lugar de un **Nested
Loop**, lo que impide a TimescaleDB podar chunks de forma eficaz.

La causa más habitual es un `random_page_cost` alto (el valor por defecto es
`4.0`, pensado para discos duros). Con almacenamiento moderno (SSD, NVMe) o una
base bien cacheada, conviene bajarlo:

```sql
-- Ver el valor actual
SHOW random_page_cost;

-- Bajarlo (habitualmente 1.1)
ALTER SYSTEM SET random_page_cost = 1.1;
SELECT pg_reload_conf();
```

Un valor más bajo anima al planificador a usar uniones por índice (Nested
Loops), esenciales para el rendimiento de Scribe con grandes volúmenes.

### ¿Sigues con problemas?
[Abre una incidencia](https://github.com/jonathan-gtd/scribe/issues) en GitHub con tus registros y tu configuración. ¡Estaré encantado de ayudar!

</details>

---

## Licencia

Licencia MIT — consulta el archivo LICENSE para más detalles

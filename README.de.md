<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="brands_assets/dark_logo.png">
  <img src="brands_assets/logo.png" alt="Scribe" width="300">
</picture>

### Home-Assistant-Historie in TimescaleDB

Jeder Zustand und jedes Ereignis, über `asyncpg` — ohne die Event-Loop zu blockieren.

[![Release](https://img.shields.io/github/v/release/jonathan-gtd/scribe?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases/latest) [![Downloads](https://img.shields.io/github/downloads/jonathan-gtd/scribe/total?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases) [![Tests](https://img.shields.io/github/actions/workflow/status/jonathan-gtd/scribe/tests.yaml?branch=master&label=tests)](https://github.com/jonathan-gtd/scribe/actions/workflows/tests.yaml) [![License](https://img.shields.io/github/license/jonathan-gtd/scribe?color=lightgrey)](LICENSE)

[![lang en](https://img.shields.io/badge/lang-en-lightgrey)](README.md) [![lang fr](https://img.shields.io/badge/lang-fr-lightgrey)](README.fr.md) [![lang es](https://img.shields.io/badge/lang-es-lightgrey)](README.es.md) [![lang de](https://img.shields.io/badge/lang-de-41BDF5)](README.de.md) [![lang nl](https://img.shields.io/badge/lang-nl-lightgrey)](README.nl.md)

</div>

---

Der Recorder von Home Assistant hält einige Wochen Historie in SQLite und wird langsamer, je größer sie wird. Scribe schreibt dieselben Zustände und Ereignisse nach **TimescaleDB**, wo Jahre schnell bleiben und einen Bruchteil des Platzes brauchen.

- 🚀 **Durchgehend asynchron** — `asyncpg` und gebündelte `COPY`: die Aufzeichnung blockiert Home Assistant nie.
- 🗜️ **Automatisch komprimiert** — alte Historie wird in Chunks geteilt und komprimiert, typischerweise 10× kleiner.
- 🛟 **Nichts geht verloren** — eine nicht erreichbare Datenbank wird gepuffert und geschrieben, sobald sie zurück ist.
- 🧩 **Mit Kontext** — Entitäten, Geräte, Bereiche, Benutzer und Integrationen, nicht nur Werte.
- 🩺 **Es sagt, wenn etwas nicht stimmt**, in Reparaturen statt in einem Log, das niemand liest.

---

## Installation

**1. Eine TimescaleDB-Datenbank.** Die Erweiterung ist erforderlich — siehe *TimescaleDB einrichten* weiter unten.

**2. Scribe, über HACS:**

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=jonathan-gtd&repository=scribe&category=integration)

*Oder von Hand:* `custom_components/scribe` in Ihren `custom_components`-Ordner kopieren. In beiden Fällen Home Assistant neu starten.

**3. Die Datenbank-URL.** Öffnen Sie **Einstellungen → Geräte & Dienste → Integration hinzufügen**, suchen Sie nach **Scribe** und fügen Sie sie ein:

[![Open your Home Assistant instance and start setting up a new integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=scribe)

```
postgresql://scribe:password@192.168.1.10:5432/scribe
```

*Oder in `configuration.yaml`*, wenn Sie Ihre Konfiguration lieber in Dateien halten:

```yaml
scribe:
  db_url: "postgresql://scribe:password@192.168.1.10:5432/scribe"
```

Fertig — Zustände werden aufgezeichnet, in Chunks geteilt und komprimiert, mit ihrem Entitäts-, Geräte- und Bereichskontext. Alles Weitere ist optional.

---

<details>
<summary><b>🧩 Scribe Card — Diagramme auf Ihrem Dashboard</b></summary>
<br>

**[Scribe Card](https://github.com/jonathan-gtd/scribe-card)** bringt jede Abfrage Ihrer Historie auf ein Dashboard. Gezeichnet mit Apache ECharts — der Bibliothek, die auch die Verlaufsdiagramme von Home Assistant nutzen — und in einem Formular konfiguriert, in dem Diagrammtyp, Einheit und Achsen aus den Spalten Ihrer Abfrage gewählt werden.

Sie spricht über den Dienst `scribe.query` mit Scribe: **keine zweite Datenbankverbindung und kein Passwort im Dashboard**. Installation über HACS als benutzerdefiniertes Repository, Kategorie *Dashboard*.

</details>

<details>
<summary><b>🗄️ TimescaleDB einrichten</b></summary>
<br>

### Datenbank einrichten

Du brauchst eine laufende TimescaleDB-Instanz. Ich empfehle PostgreSQL 17 oder 18.

> **❗ Wichtig** — **Die TimescaleDB-Erweiterung ist erforderlich.** Chunking, Komprimierung,
> Aufbewahrung und die Größen-Sensoren sind der eigentliche Zweck von Scribe —
> auf reinem PostgreSQL gibt es davon nichts. Eine neue Installation wird
> abgelehnt, wenn die Erweiterung fehlt; Scribe aktiviert sie allerdings selbst,
> sofern der Server sie bereitstellt und dein Datenbankbenutzer `CREATE` auf der
> Datenbank besitzt — was die untenstehende Einrichtung gewährt. Bereits
> laufende Installationen ohne die Erweiterung zeichnen weiter auf und erfahren
> über einen Eintrag unter Reparaturen, was ihnen fehlt.

#### Variante A: Home Assistant OS (Add-on)

Unter Home Assistant OS empfehle ich das [TimescaleDB-Add-on](https://github.com/expaso/hassos-addon-timescaledb).

[![Öffne deine Home-Assistant-Instanz und zeige den Dialog zum Hinzufügen eines Add-on-Repositorys mit vorausgefüllter URL.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Fexpaso%2Fhassos-addon-timescaledb)

#### Variante B: Docker (manuell)

```bash
# Hochverfügbarkeit (empfohlen)
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg18

# Standard
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:pg18
```

Datenbank und Benutzer anlegen:

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
<summary><b>⚙️ Alle Optionen, mit ihren Standardwerten</b></summary>
<br>

### Vollständige Konfiguration (Standardwerte)

```yaml
scribe:
  # Die einzige erforderliche Option.
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe

  # Alles Weitere ist optional. Dies sind die Standardwerte.

  # Wohin es schreibt
  db_schema: ""                 # leer = das Schema der Verbindung, normalerweise public
  db_ssl: false                 # TLS zur Datenbank
  ssl_root_cert: ""             # CA-Zertifikat; nur gelesen, wenn db_ssl true ist
  ssl_cert_file: ""             # Client-Zertifikat, für gegenseitiges TLS
  ssl_key_file: ""              # dessen privater Schlüssel

  # Was es aufzeichnet
  record_states: true           # Zustandsänderungen
  record_events: false          # Home-Assistant-Ereignisse (Automationen, Skripte…)
  include_domains: []           # leer = alle Domains
  include_entities: []          # leer = alle Entitäten
  include_entity_globs: []      # z. B. sensor.wetter_*
  exclude_domains: []           # wird nach den Include-Listen angewendet
  exclude_entities: []
  exclude_entity_globs: []
  exclude_attributes: []        # Attribute, die aus der Spalte attributes entfernt werden
  include_events: []            # leer = alle Ereignistypen
  exclude_events: []            # wird nach include_events angewendet

  # Wie lange es sie behält
  chunk_time_interval: "7 days" # Zeitspanne, die ein Chunk abdeckt
  compress_after: "7 days"      # ältere Chunks werden komprimiert
  retention_states: ""          # leer = für immer; sonst LÖSCHT es ältere Zustände
  retention_events: ""          # leer = für immer; sonst LÖSCHT es ältere Ereignisse
  enable_rollups: false         # stündliche und tägliche Zusammenfassungen numerischer Zustände

  # Wie es schreibt
  batch_size: 500               # Zeilen im Puffer vor einem Schreibvorgang
  flush_interval: 30            # Sekunden, bevor ein unvollständiger Stapel geschrieben wird
  max_queue_size: 10000         # Zeilen im Speicher, bevor neue verworfen werden
  buffer_on_failure: true       # weiter puffern, solange die Datenbank nicht erreichbar ist

  # Was scribe.query kosten darf
  query_timeout: 60             # Sekunden, die eine Abfrage laufen darf
  query_max_rows: 20000         # Zeilen, die sie zurückgeben darf, bevor sie abgelehnt wird

  # Sensoren über Scribe selbst
  enable_stats_io: false        # Writer-Zähler, aus dem Speicher gelesen
  enable_stats_chunk: false     # Chunk-Anzahl, eine Abfrage pro Aktualisierung
  enable_stats_size: false      # Größen auf der Festplatte, eine Abfrage pro Aktualisierung
  stats_io_interval: 60         # Sekunden zwischen zwei Schreibwerten
  stats_chunk_interval: 60      # Minuten zwischen zwei Chunk-Abfragen
  stats_size_interval: 60       # Minuten zwischen zwei Größen-Abfragen

  # Kontext-Tabellen, synchron mit den Registern von Home Assistant
  enable_table_areas: true
  enable_table_devices: true
  enable_table_integrations: true
  enable_table_users: true
```

</details>

<details>
<summary><b>📋 Parameter-Referenz</b></summary>
<br>

| Parameter | Beschreibung |
| :--- | :--- |
| `db_url` | **Erforderlich.** Verbindungszeichenfolge zu deiner TimescaleDB-Datenbank. |
| `db_ssl` | SSL/TLS für die Datenbankverbindung aktivieren. |
| `ssl_root_cert` | Pfad zur CA-Datei (z. B. `/ssl/ca.crt`). Ein relativer Pfad wird vom Konfigurationsverzeichnis von Home Assistant aus aufgelöst. |
| `ssl_cert_file` | Pfad zum Client-Zertifikat, für gegenseitiges TLS. |
| `ssl_key_file` | Pfad zum privaten Client-Schlüssel, für gegenseitiges TLS. |
| `db_schema` | PostgreSQL-Schema, in das geschrieben wird. Leer (Standard): das der Verbindung, normalerweise `public`. |
| `chunk_time_interval` | Welchen Zeitraum ein Chunk der Tabelle abdeckt. Siehe *Speicher-Feinabstimmung* weiter unten. |
| `compress_after` | Chunks, die älter sind als dieses Intervall, werden komprimiert. Siehe *Speicher-Feinabstimmung* weiter unten. |
| `retention_states` | **Löscht** Zustandsverlauf, der älter ist als dieses Intervall (z. B. `"365 days"`). Leer (Standard) behält alles. Siehe *Aufbewahrung* weiter unten. |
| `retention_events` | **Löscht** Ereignisverlauf, der älter ist als dieses Intervall. Leer (Standard) behält alles. Siehe *Aufbewahrung* weiter unten. |
| `record_states` | Ob Zustandsänderungen aufgezeichnet werden. |
| `record_events` | Ob Ereignisse aufgezeichnet werden. |
| `batch_size` | Anzahl der Einträge, die gepuffert werden, bevor in die Datenbank geschrieben wird. |
| `flush_interval` | Sekunden, bevor ein unvollständiger Stapel trotzdem geschrieben wird (Standard `30`). Jeder Schreibvorgang ist eine Transaktion: ein kurzes Intervall schreibt jeweils eine Handvoll Zeilen, statt sie zu bündeln, ein langes riskiert nur das letzte Intervall an Historie — und auch nur, wenn Home Assistant abgeschossen wird. |
| `max_queue_size` | Maximale Anzahl an Einträgen im Speicher, bevor neue verworfen werden. |
| `query_timeout` | Sekunden, die ein `scribe.query`-Aufruf laufen darf, bevor die Datenbank ihn beendet (Standard `60`). |
| `query_max_rows` | Zeilen, die ein `scribe.query`-Aufruf zurückgeben darf, bevor er abgelehnt wird (Standard `20000`). |
| `buffer_on_failure` | Wenn wahr, bleiben Daten im Speicher, solange die Datenbank nicht erreichbar ist (bis `max_queue_size`). |
| `enable_stats_io` | Echtzeit-Sensoren zur Schreibleistung aktivieren (ohne Datenbankabfragen). |
| `enable_stats_chunk` | Sensoren für die Chunk-Anzahl aktivieren (fragen die Datenbank ab). |
| `enable_stats_size` | Sensoren für den Speicherverbrauch aktivieren (fragen die Datenbank ab). |
| `stats_io_interval` | Sekunden zwischen zwei Werten der E/A-Sensoren (Standard `60`). Jede Änderung ist eine Zeile, die Scribe über sich selbst aufzeichnet. |
| `stats_chunk_interval` | Aktualisierungsintervall (in Minuten) der Chunk-Statistiken. |
| `stats_size_interval` | Aktualisierungsintervall (in Minuten) der Größenstatistiken. |
| `include_domains` | Liste der einzuschließenden Domains. |
| `include_entities` | Liste einzelner einzuschließender Entitäten. |
| `include_entity_globs` | Liste einzuschließender Entitätsmuster (z. B. `sensor.weather_*`). |
| `exclude_domains` | Liste der auszuschließenden Domains. |
| `exclude_entities` | Liste einzelner auszuschließender Entitäten. |
| `exclude_entity_globs` | Liste auszuschließender Entitätsmuster (z. B. `switch.kitchen_*`). |
| `exclude_attributes` | Liste von Attributen, die aus der Spalte `attributes` ausgeschlossen werden. |
| `include_events` | Liste der aufzuzeichnenden Ereignistypen. Leer lassen, um alle aufzuzeichnen. |
| `exclude_events` | Liste der nie aufzuzeichnenden Ereignistypen (wird nach `include_events` angewendet). |
| `enable_table_areas` | Anlegen und Synchronisieren der Tabelle `areas` aktivieren. |
| `enable_table_devices` | Anlegen und Synchronisieren der Tabelle `devices` aktivieren. |
| `enable_table_integrations` | Anlegen und Synchronisieren der Tabelle `integrations` aktivieren. |
| `enable_table_users` | Anlegen und Synchronisieren der Tabelle `users` aktivieren. |
| `enable_rollups` | Vorberechnete stündliche und tägliche Zusammenfassungen der Zustände vorhalten (`states_hourly`, `states_daily`). Standardmäßig aus. |

</details>

<details>
<summary><b>🗜️ Speicher-Feinabstimmung — Chunks und Kompression</b></summary>
<br>

Scribe legt den Verlauf in **Hypertables** von TimescaleDB ab: eine Tabelle, die
sich wie jede andere verhält und abfragen lässt, physisch aber in **Chunks**
zerlegt ist, von denen jeder einen Zeitabschnitt abdeckt. Fast alles, was
Plattenplatz und Abfragegeschwindigkeit betrifft, folgt aus dieser Aufteilung:
Eine Abfrage über die letzte Woche liest nur die Chunks, die sie überlappen, die
Komprimierung arbeitet Chunk für Chunk, und die *Aufbewahrung* weiter unten
löscht ganze Chunks statt einzelner Zeilen.

Gesteuert wird das von zwei Einstellungen, in YAML wie in der Oberfläche unter
**Konfigurieren → Erweitert (TimescaleDB & SSL)**:

### `chunk_time_interval` (Standard `7 days`)

Welchen Zeitraum ein Chunk abdeckt.

- **Kleinere Chunks** (z. B. `1 day`) bedeuten mehr, dafür kleinere Dateien:
  feinere Aufbewahrung, und Abfragen über kurze, aktuelle Zeitfenster berühren
  weniger Daten. Ab einem gewissen Punkt muss eine Abfrage über mehrere Monate
  Hunderte von Chunks öffnen.
- **Größere Chunks** (z. B. `30 days`) bedeuten weniger, dafür größere Dateien:
  besser für lange historische Abfragen, schlechter für den Arbeitsspeicher.
  TimescaleDB selbst empfiehlt, dass die Chunks, in die geschrieben wird,
  zusammen mit ihren Indizes bequem in den Speicher passen — ein übergroßer
  Chunk auf einer kleinen Maschine bremst die Schreibvorgänge.

Der Standardwert passt zu einer typischen Home-Assistant-Instanz. `1 day` ist
eine Überlegung wert, wenn du Tausende von Entitäten aufzeichnest — und nur
dann.

> **Eine Änderung betrifft nur neue Chunks.** Bereits geschriebene Chunks
> behalten den Zeitraum, mit dem sie angelegt wurden; nichts wird neu geschrieben
> oder verschoben. Du hast dann schlicht eine Mischung aus alten und neuen
> Zeiträumen, womit TimescaleDB von Haus aus umgeht.

### `compress_after` (Standard `7 days`)

Wie alt ein Chunk sein muss, bevor TimescaleDB ihn komprimiert. Bei dieser Art
von Daten (viele wiederholte `entity_id`s und sich langsam ändernde Werte) fällt
die Größenersparnis in der Regel deutlich aus — deshalb ist die Komprimierung
standardmäßig aktiv.

Komprimierte Chunks bleiben vollständig abfragbar — der Sicht `states` ist das
gleichgültig. Das Schreiben *in* einen solchen Chunk ist langsamer, weshalb die
Komprimierung erst greift, wenn ein Chunk alt genug ist, um praktisch
abgeschlossen zu sein. Halte `compress_after` deutlich über dem Alter der Daten,
die du noch schreibst: Zustände, die verspätet eintreffen (ein Nachtrag, ein
Migrationsskript), landen in alten Chunks.

> **Eine Änderung wirkt beim nächsten Neustart**, und bereits komprimierte
> Chunks bleiben komprimiert — die Einstellung bestimmt nur, wann die
> *nächsten* komprimiert werden.

### Wie die drei Einstellungen zusammenspielen

| Einstellung | Was sie bewirkt | Umkehrbar |
| :--- | :--- | :--- |
| `chunk_time_interval` | Welchen Zeitraum ein Chunk abdeckt | Ja — nur künftige Chunks |
| `compress_after` | Wann ein Chunk komprimiert wird | Ja |
| `retention_states` / `retention_events` | Wann ein Chunk **gelöscht** wird | **Nein** |

Sie greifen in dieser Reihenfolge im Leben desselben Chunks: geschrieben →
komprimiert → gelöscht. Zwei Folgerungen sind wichtig:

- Ist `compress_after` größer als deine Aufbewahrung, werden Chunks gelöscht,
  bevor sie je komprimiert wurden — die Komprimierung bewirkt dann nichts.
- Die Aufbewahrung löscht ganze Chunks: Dein tatsächliches Zeitfenster ist also
  das eingestellte Intervall **plus** bis zu einem `chunk_time_interval`.
  Kleinere Chunks machen es genauer.

Sind die Größen- und Chunk-Sensoren aktiviert (`enable_stats_size`,
`enable_stats_chunk`), zeigen sie genau das, was diese Einstellungen bewirken:
Chunk-Anzahl, komprimierte und unkomprimierte Größen sowie die
Komprimierungsrate.

</details>

<details>
<summary><b>🧹 Aufbewahrung — alte Historie planmäßig löschen</b></summary>
<br>

Standardmäßig behält Scribe alles, unbegrenzt. Wenn du nur ein begrenztes
Zeitfenster speichern willst — weil du den Rohverlauf anderswo aggregierst oder
schlicht den Plattenplatz deckeln möchtest —, setze ein Aufbewahrungsintervall,
und TimescaleDB verwirft ältere Chunks:

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  retention_states: "365 days"
  retention_events: "30 days"
```

Beides gibt es auch in der Oberfläche unter
**Konfigurieren → Erweitert (TimescaleDB & SSL)**.

> **⚠️ Achtung** — Die Aufbewahrung **löscht Daten endgültig**. Es gibt kein Rückgängig und
> keinen Papierkorb: Sobald ein Chunk aus dem Fenster fällt, wird er gelöscht,
> und nur eine Sicherung bringt ihn zurück. Zustände und Ereignisse werden
> getrennt konfiguriert, sodass du lärmende Ereignisse verfallen lassen und den
> Zustandsverlauf behalten kannst.

Wissenswertes:

- **Keine Einstellung heißt immer „unbegrenzt aufbewahren“.** Das Feld in der
  Oberfläche zu leeren und die Zeile aus `configuration.yaml` zu löschen
  entfernen beide die Richtlinie — ein einst aus YAML übernommener Wert darf
  niemals die Zeile überleben, die ihn gesetzt hat.
- **Scribe besitzt die Aufbewahrungsrichtlinie auf seinen eigenen Tabellen.**
  Das Leeren des Feldes entfernt sie — auch eine, die du selbst mit
  `add_retention_policy()` angelegt hast. Nur so kann das Leeren der Einstellung
  in der Oberfläche das Löschen tatsächlich stoppen.
- **Sie beginnt sofort.** TimescaleDB führt die Richtlinie schon Sekunden nach
  ihrer Erstellung aus, nicht erst beim nächsten Tagesintervall: Alles außerhalb
  des Fensters ist beim ersten Lauf weg, direkt nach dem Neustart, der sie
  aktiviert hat.
- **Gelöscht wird chunkweise, nicht zeilenweise.** Ein Chunk wird erst gelöscht,
  wenn *alle* seine Zeilen älter als das Intervall sind — mit dem
  Standardwert `chunk_time_interval` von 7 Tagen behältst du also bis zu eine
  Woche mehr als gewünscht. Genau das macht die Aufbewahrung nahezu kostenlos:
  Sie verwirft Dateien, statt Zeilen zu löschen.
- **Nur der Verlauf wird gelöscht.** Die Tabelle `entities` und die übrigen
  Metadaten-Tabellen bleiben unangetastet: Eine Entität, deren Verlauf
  vollständig abgelaufen ist, lässt sich weiterhin auflösen.
- **TimescaleDB ist erforderlich** — die Erweiterung führt die Richtlinie aus.
  Auf reinem PostgreSQL erzeugt ein gesetztes Aufbewahrungsintervall einen
  Eintrag unter Reparaturen, statt stillschweigend nichts zu tun.
- Zulässig sind einfache Intervalle: `30 days`, `6 months`, `1 year`. Alles
  andere wird mit einem Fehler abgelehnt, statt an die Datenbank geschickt zu
  werden.

</details>

<details>
<summary><b>📈 Zusammenfassungen — stündliche und tägliche Aggregate</b></summary>
<br>

Ein Jahr eines Sensors, der alle 30 Sekunden meldet, sind rund eine Million Zeilen. Ein Diagramm dieses Jahres liest sie alle — jedes Mal, wenn es gezeichnet wird.

Mit `enable_rollups: true` hält TimescaleDB zwei Zusammenfassungen Ihrer Zustände laufend aktuell — stündlich und täglich — und ein Diagramm über Jahre liest Tausende Zeilen statt Millionen.

```yaml
scribe:
  enable_rollups: true
```

Auch in der Oberfläche unter **Konfigurieren → Metadaten-Tabellen**. Das ergänzt zwei Sichten:

| Sicht | Eine Zeile je | Spalten |
| --- | --- | --- |
| `states_hourly` | Entität und Stunde | `entity_id`, `bucket`, `value_avg`, `value_min`, `value_max`, `samples` |
| `states_daily` | Entität und Tag | dieselben |

```sql
SELECT bucket, value_avg, value_min, value_max
FROM states_daily
WHERE entity_id = 'sensor.aussentemperatur'
  AND bucket > now() - interval '2 years'
ORDER BY bucket;
```

Zusammengefasst werden nur numerische Zustände — der Mittelwert von `on` und `off` bedeutet nichts — deshalb bleiben `value_avg`, `value_min` und `value_max` für alle anderen leer, während `samples` jeden Zustand im Bucket zählt.

**Es sind abgeleitete Daten.** Nichts, was Sie vermissen würden, wird doppelt gehalten: Ausschalten löscht beide Sichten, Wiedereinschalten baut sie aus der Historie neu auf, und Ihre Zustände bleiben in beiden Fällen unberührt. TimescaleDB aktualisiert sie selbst — die stündliche alle 30 Minuten, die tägliche jede Stunde — und jeder Lauf schaut weit genug zurück (3 Tage, 30 Tage), dass auch ein spät geschriebener Stapel darin landet. Scribe legt sie nur an.

</details>

<details>
<summary><b>🗃️ In ein bestimmtes PostgreSQL-Schema schreiben</b></summary>
<br>

Standardmäßig schreibt Scribe in das Schema, auf das Ihre Verbindung ohnehin zeigt — normalerweise `public`. Setzen Sie `db_schema`, und es legt dieses Schema an und packt alles hinein: Tabellen, Sichten, Hypertables und Richtlinien.

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  db_schema: scribe
```

Auch in der Oberfläche unter **Konfigurieren → Erweitert (TimescaleDB & SSL)**.

Das wollen Sie, wenn Scribe sich eine Datenbank mit etwas anderem teilt: den Tabellen einer anderen Integration, Ihren eigenen Kopien der Historie oder einem zweiten Home Assistant auf demselben Server. Schemata sind unabhängig — eigene Tabellen, Hypertables, Aufbewahrung und Kompression — und nichts, was Scribe im einen tut, erreicht das andere.

- **Nur neue Daten landen dort.** `db_schema` zu setzen verschiebt bereits aufgezeichnete Historie nicht. Verschieben Sie sie vor dem Neustart selbst (`ALTER TABLE public.states_raw SET SCHEMA scribe;`) oder fragen Sie das alte Schema direkt ab.
- **Scribe legt das Schema an, wenn es darf** — dafür braucht der Benutzer `CREATE` auf der Datenbank. Ein von Hand angelegtes Schema geht ebenso, mit `USAGE` und `CREATE` darauf.
- **Ein unerreichbares Schema stoppt die Aufzeichnung.** PostgreSQL fällt auf den nächsten Eintrag des Search Path zurück, statt zu scheitern; ein Tippfehler würde also `public` füllen, während die Oberfläche etwas anderes zeigt. Scribe prüft, wo es gelandet ist, und schreibt lieber nichts als am falschen Ort — mit einem Eintrag in Reparaturen, der sagt, was zu gewähren ist.
- **Ihre Abfragen ändern sich nicht.** Scribe stellt das Schema an den Anfang des `search_path` der Verbindung, `SELECT * FROM states` funktioniert über `scribe.query` weiter. Aus Grafana oder psql qualifizieren Sie den Namen (`scribe.states`) oder setzen Ihren eigenen `search_path`. `public` bleibt auf dem Pfad — dort liegen die TimescaleDB-Funktionen.
- Zulässig sind einfache Bezeichner: Buchstaben, Ziffern und Unterstriche, nicht mit einer Ziffer beginnend. Leer behält das Schema der Verbindung, auch eines, das Sie selbst mit `?options=-csearch_path%3Dmeinschema` in der URL gesetzt haben.

**Die Tabellen selbst** — jede Spalte, ihre Beziehungen und Abfragerezepte für Grafana und `scribe.query` — sind in [`docs/data-structure.md`](docs/data-structure.md) dokumentiert.

</details>

<details>
<summary><b>🛠️ Dienste — flush, query, purge</b></summary>
<br>

### `scribe.flush`
Erzwingt das sofortige Schreiben der gepufferten Daten in die Datenbank.

```yaml
service: scribe.flush
```

### `scribe.query`
Führt eine reine Leseabfrage (SQL) gegen die TimescaleDB-Datenbank aus.

**Parameter:**
- `sql` (erforderlich): die auszuführende SQL-Abfrage. Muss eine `SELECT`-Anweisung sein.

**Rückgabe:**
Eine Liste von Zeilen, wobei jede Zeile ein Wörterbuch aus Spaltennamen und Werten ist.

**Beispiel:**
```yaml
service: scribe.query
data:
  sql: "SELECT * FROM states ORDER BY time DESC LIMIT 5"
response_variable: query_result
```

### `scribe.purge`
Löscht aufgezeichnete Historie. **Das lässt sich nicht rückgängig machen.**

**Parameter** (mindestens einer der ersten beiden ist erforderlich):
- `entity_id`: die zu löschenden Entitäten. Ohne `keep_days` werden ihre gesamte Historie *und* ihre Zeile in der Tabelle `entities` gelöscht — zeichnet man sie erneut auf, beginnt alles von vorn.
- `keep_days`: löscht alles, was älter ist als diese Anzahl Tage.
- `events` (Standard `false`): löscht auch Ereignisse, die älter sind als `keep_days`. Ohne diese Angabe wird die Option ignoriert.

**Gibt zurück:** wie viele Zustände, Ereignisse und Entitätszeilen gelöscht wurden.

**Beispiele:**
```yaml
# Eine Entität vollständig aus der Datenbank entfernen
action: scribe.purge
data:
  entity_id: sensor.sensor_den_ich_nicht_mehr_will
```

```yaml
# Alles älter als zwei Jahre entfernen, Ereignisse eingeschlossen
action: scribe.purge
data:
  keep_days: 730
  events: true
response_variable: purged
```

Komprimierte Historie wird ebenfalls gelöscht: TimescaleDB erledigt das, und die Chunks bleiben komprimiert. Für ein gleitendes Fenster, das dauerhaft gilt, nutzen Sie stattdessen die Einstellungen unter *Aufbewahrung* weiter unten: eine Purge ist einmalig.

</details>

<details>
<summary><b>📊 Statistik-Sensoren</b></summary>
<br>

Aktiviere die Sensoren, indem du ihre Optionen in der Konfiguration setzt.

### Schreibstatistiken (`enable_stats_io: true`)

Echtzeitwerte aus dem Writer (ohne Datenbankabfragen).

| Sensor | Beschreibung |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_states_written` | Gesamtzahl der in die Datenbank geschriebenen Zustandsänderungen. |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_events_written` | Gesamtzahl der in die Datenbank geschriebenen Ereignisse. |
| <img src="https://api.iconify.design/mdi:buffer.svg?color=%232196F3" width="15" /> `sensor.scribe_buffer_size` | Aktuelle Anzahl der Einträge im Speicherpuffer. |
| <img src="https://api.iconify.design/mdi:timer-sand.svg?color=%232196F3" width="15" /> `sensor.scribe_last_write_duration` | Dauer (in ms) des letzten Schreibvorgangs. |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_states_rate` | Rate der geschriebenen Zustände (pro Minute). |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_events_rate` | Rate der geschriebenen Ereignisse (pro Minute). |

### Chunk-Statistiken (`enable_stats_chunk: true`)

Chunk-Anzahl (alle `stats_chunk_interval` Minuten aktualisiert).

| Sensor | Beschreibung |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_chunks` | Gesamtzahl der Chunks der Zustandstabelle. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_chunks` | Anzahl der bereits komprimierten Chunks. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_chunks` | Anzahl der Chunks, die auf Komprimierung warten. |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_chunks` | Gesamtzahl der Chunks der Ereignistabelle. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_chunks` | Anzahl der komprimierten Ereignis-Chunks. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_chunks` | Anzahl der unkomprimierten Ereignis-Chunks. |

### Größenstatistiken (`enable_stats_size: true`)

Belegter Speicher in Bytes (alle `stats_size_interval` Minuten aktualisiert).

| Sensor | Beschreibung |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_size` | Gesamtgröße auf der Platte (komprimierte Daten + aktuelle Chunks + Indizes). |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_states_original_size` | **Theoretische Größe** ohne Komprimierung (z. B. 11 GB). |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_size` | Physische Größe der komprimierten Daten-Chunks. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_size` | Größe der noch nicht komprimierten aktuellen Daten (oder ausstehender Indizes). |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compression_ratio` | Komprimierungsrate der Zustände (%). |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_size` | Gesamtgröße der Ereignistabelle auf der Platte. |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_events_original_size` | Theoretische Größe der Ereignisse vor der Komprimierung. |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_size` | Größe der komprimierten Ereignisdaten. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_size` | Größe der unkomprimierten Ereignisdaten. |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compression_ratio` | Komprimierungsrate der Ereignisse (%). |

</details>

<details>
<summary><b>🖼️ Dashboard</b></summary>
<br>

Ein vorbereitetes Lovelace-Layout mit allen nützlichen Scribe-Sensoren
(Datenbankstatistiken, Komprimierungsraten, Schreibleistung) liegt in diesem
Repository, in zwei Varianten:

| Datei | Was es ist | Wohin einfügen |
| --- | --- | --- |
| [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml) | Eine **einzelne Karte** (`type: vertical-stack`) | Der YAML-Editor einer Karte („Karte hinzufügen“ → „Manuell“) |
| [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml) | Eine **ganze Ansicht** (`title` / `icon` / `cards`) | Der YAML-Editor einer Ansicht |

> ⚠️ Die beiden sind nicht austauschbar. Die *Ansicht*-Datei in einen
> *Karten*-Editor einzufügen scheitert mit **„No card type configured“**, denn
> eine Kartenkonfiguration muss mit einem `type:`-Schlüssel beginnen.

**Variante A — als Karte hinzufügen (am einfachsten, funktioniert in jedem Ansichtstyp):**

1.  Öffne dein Dashboard und klicke auf „Dashboard bearbeiten“ (Stiftsymbol).
2.  Klicke auf **+ Karte hinzufügen** und wähle ganz unten in der Auswahl **Manuell**.
3.  Kopiere den Inhalt von [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml), ersetze damit alles im Editor und klicke auf **Speichern**.

**Variante B — als eigene Ansicht hinzufügen:**

1.  Öffne dein Dashboard und klicke auf „Dashboard bearbeiten“ (Stiftsymbol).
2.  Klicke auf die Schaltfläche **+** *in der oberen Reiterleiste* (neben deinen vorhandenen Ansichten), um eine Ansicht hinzuzufügen — nicht auf „Karte hinzufügen“.
3.  Öffne im Ansichtsdialog das Menü ⋮ (oder die Schaltfläche „Code-Editor anzeigen“) und wähle **In YAML bearbeiten**.
4.  Kopiere den Inhalt von [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml), ersetze damit alles im Editor und klicke auf **Speichern**.

</details>

<details>
<summary><b>📦 Migration von InfluxDB, LTSS, dem Recorder oder Scribe 2.x</b></summary>
<br>

### Aktualisierung von Scribe 2.x

Scribe 3.0 ersetzte die Tabelle `states` durch `states_raw` plus eine
Kompatibilitäts-Sicht und gab `entities` einen numerischen Primärschlüssel. Die
Umwandlung einer alten Datenbank trugen die 3.x-Versionen; **in 3.9 wurde sie
entfernt**.

Wenn deine Datenbank noch eine `states`-*Tabelle* (statt einer Sicht), eine
Tabelle `states_legacy` oder eine Tabelle `entities` ohne Spalte `id` enthält,
hält Scribe beim Start an, zeichnet nichts auf und meldet einen Eintrag unter
Reparaturen — ohne irgendetwas umzubenennen, anzulegen oder zu löschen.
Installiere **Scribe 3.8**, lass Home Assistant laufen, bis das Protokoll die
abgeschlossene Migration meldet (bei einer großen Datenbank rund fünfzehn
Minuten), und aktualisiere dann erneut.

Neuinstallationen und jede von 3.x angelegte Datenbank sind nicht betroffen.

### Daten aus anderen Quellen übernehmen

In `migration/` liegen drei Skripte, die Historie von anderswo nach Scribe kopieren. Sie werden einmalig von Hand ausgeführt, auf einem Rechner, der beide Datenbanken erreicht — und Scribe muss mindestens einmal gestartet sein, damit seine Tabellen existieren.

```bash
cd migration
pip install psycopg2-binary python-dotenv   # für InfluxDB zusätzlich influxdb-client
cp .env.example .env && nano .env
python3 <script>.py
```

| Quelle | Skript | Auszufüllen |
| --- | --- | --- |
| InfluxDB | `influx2scribe.py` | `INFLUX_*` |
| LTSS | `ltss2scribe.py` | `LTSS_*` |
| Home-Assistant-Recorder | `recorder2scribe.py` | `RECORDER_*`, mit `RECORDER_TYPE` auf `postgres` oder `sqlite` (SQLite braucht nur `RECORDER_DB_PATH`) |

Jeder Lauf braucht außerdem `SCRIBE_*` — das Ziel — und die Migrationseinstellungen: `MIGRATION_START_TIME`, `MIGRATION_END_TIME`, `CHUNK_SIZE` (Stunden pro Stapel) und `PURGE_DESTINATION`, das **die Historie des Ziels vor dem Import löscht**. Lassen Sie es auf `False`, sofern Sie es nicht ausdrücklich wollen.

Jedes Skript prüft das Zielschema, bevor es irgendetwas schreibt, und hält mit einer Erklärung an statt mit einer Mauer aus Fehlern Zeile für Zeile, falls Scribe es nie initialisiert hat.

</details>

<details>
<summary><b>🩺 Fehlerbehebung</b></summary>
<br>

### Was man zuerst ansieht

Zwei Stellen beantworten „warum wird nichts aufgezeichnet?“, ohne eine einzige Protokollzeile zu lesen:

- **Einstellungen → Geräte & Dienste → Scribe → ⋮ → Diagnose herunterladen** berichtet, was
  der Writer tatsächlich tut: verbunden oder nicht, ob TimescaleDB gefunden wurde, wie viele
  Einträge im Puffer warten und wie viele verworfen wurden, aufeinanderfolgende Schreibfehler
  sowie die geltenden Speicher- und Aufbewahrungseinstellungen. Die Datenbank-URL erscheint
  nie, und aus Treiberfehlern wird jede Verbindungszeichenfolge entfernt.
- **Einstellungen → System → Reparaturen** listet die Punkte unten auf, und
  **Einstellungen → System → Systemzustand** zeigt, auf welche Datenbank Scribe zeigt und
  ob es gerade verbunden ist.

### Reparaturen

Scribe meldet Probleme, die es nicht selbst lösen kann, unter
**Einstellungen → System → Reparaturen**, damit du nicht die Protokolle
beobachten musst. Jeder Eintrag verschwindet von selbst, sobald die Ursache
behoben ist.

| Reparatur | Was sie bedeutet |
| --- | --- |
| Datenbank nicht erreichbar | Die Verbindung schlug fehl. Scribe puffert weiter und versucht es im Hintergrund erneut, sodass der während des Ausfalls aufgezeichnete Verlauf geschrieben wird, sobald die Datenbank zurück ist. Prüfe, ob der Server läuft und ob URL und Zugangsdaten stimmen. |
| Schreiben in die Datenbank nicht möglich | Mehrere Schreibvorgänge in Folge sind fehlgeschlagen. Die Daten bleiben im Speicher und werden nach der Erholung geschrieben — sofern Home Assistant nicht vorher neu startet. |
| Puffer ist voll | Die Schreibfehler dauerten lange genug an, um den Puffer zu füllen; die ältesten Einträge werden nun verworfen. Repariere die Datenbank oder erhöhe `max_queue_size`. |
| Einträge werden verworfen | Ein Schreibvorgang schlug fehl, während die Pufferung deaktiviert ist — die Einträge gingen sofort verloren. Aktiviere die Pufferung, um kurze Ausfälle zu überstehen. |
| Tabellen konnten nicht angelegt werden | Scribe hat die Datenbank erreicht, konnte sein Schema aber nicht aufbauen, meist ein Rechteproblem. Auf einer neuen Datenbank wird überhaupt nichts aufgezeichnet. |
| Das angegebene Schema ist nicht erreichbar | Das Schema aus `db_schema` existiert nicht und konnte nicht angelegt werden, oder der Datenbankbenutzer hat keine Rechte darauf. Es wird nichts aufgezeichnet — statt still `public` zu füllen. |
| Sicht `states` konnte nicht angelegt werden | Der Verlauf wird aufgezeichnet, doch die Sicht, über die jede Abfrage läuft, fehlt — der Verlauf wirkt leer, obwohl nichts verloren ist. |
| `states_raw` / `events` ist keine Hypertable | TimescaleDB ist installiert, die Tabelle wurde aber nie umgewandelt (häufig, wenn die Erweiterung *nach* dem Befüllen der Tabellen hinzukam). Chunks, Komprimierung und Aufbewahrung bewirken nichts. |
| `states_raw` / `events` wird nie komprimiert | Die Tabelle ist zwar eine Hypertable, hat aber keine Komprimierungsrichtlinie und behält ihre unkomprimierte Größe. |
| TLS nicht vollständig wirksam | Scribe verbindet sich über TLS, aber ein konfiguriertes Zertifikat konnte nicht angewendet werden — meist ein Client-Zertifikat: Scribe authentifiziert sich dann als gewöhnlicher Client statt als der bereitgestellte. |
| TimescaleDB ist nicht installiert | Der Verlauf wird aufgezeichnet, aber Chunking und Komprimierung stehen nicht zur Verfügung: Die Datenbank wächst deutlich schneller und die Größen-Sensoren bleiben leer. |
| Datenbank älter als Version 3.0 | Die Datenbank nutzt noch das Schema vor 3.0, das diese Version nicht umwandeln kann. Es wird nichts aufgezeichnet und nichts verändert — installiere Scribe 3.8 zur Umwandlung und aktualisiere danach erneut. |
| Aufbewahrungsrichtlinie nicht angewendet | Du hast das Löschen von Daten ab einem Intervall verlangt, die Richtlinie konnte aber nicht angelegt werden. Es wurde nichts gelöscht und es wird nichts gelöscht — die Tabelle wächst weiter. |
| Umbenennung einer Entität nicht angewendet | Eine Umbenennung kollidierte mit einer bereits vorhandenen Zeile. Der Verlauf der Entität verteilt sich auf zwei Kennungen. |

### Hoher Speicherverbrauch
- `max_queue_size` verringern
- `flush_interval` verringern, damit der Puffer häufiger geleert wird
- `sensor.scribe_buffer_size` im Auge behalten

### Leistungsoptimierung

Ist die Sicht `states` langsam (mehrere Sekunden pro Abfrage), wählt der
PostgreSQL-Planer meist einen **Hash Join** statt eines **Nested Loop**, was
TimescaleDB daran hindert, Chunks wirksam auszuschließen.

Häufigste Ursache ist ein hoher `random_page_cost` (Standard `4.0`, auf
Festplatten ausgelegt). Bei modernem Speicher (SSD, NVMe) oder einer gut
zwischengespeicherten Datenbank solltest du diesen Wert senken:

```sql
-- Aktuellen Wert anzeigen
SHOW random_page_cost;

-- Auf einen niedrigeren Wert setzen (meist 1.1)
ALTER SYSTEM SET random_page_cost = 1.1;
SELECT pg_reload_conf();
```

Ein niedrigerer Wert bewegt den Planer zu indexbasierten Verknüpfungen (Nested
Loops), die für Scribes Leistung bei großen Datenmengen entscheidend sind.

### Immer noch Probleme?
[Öffne bitte ein Issue](https://github.com/jonathan-gtd/scribe/issues) auf GitHub mit deinen Protokollen und deiner Konfiguration. Ich helfe gerne!

</details>

---

## Lizenz

MIT-Lizenz — Einzelheiten in der Datei LICENSE

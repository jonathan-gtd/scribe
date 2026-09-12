<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="brands_assets/dark_logo.png">
  <img src="brands_assets/logo.png" alt="Scribe" width="300">
</picture>

### Home Assistant-historie in TimescaleDB

Elke toestand en elke gebeurtenis, via `asyncpg` — zonder de event loop te blokkeren.

[![Release](https://img.shields.io/github/v/release/jonathan-gtd/scribe?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases/latest) [![Downloads](https://img.shields.io/github/downloads/jonathan-gtd/scribe/total?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases) [![Tests](https://img.shields.io/github/actions/workflow/status/jonathan-gtd/scribe/tests.yaml?branch=master&label=tests)](https://github.com/jonathan-gtd/scribe/actions/workflows/tests.yaml) [![License](https://img.shields.io/github/license/jonathan-gtd/scribe?color=lightgrey)](LICENSE)

[![lang en](https://img.shields.io/badge/lang-en-lightgrey)](README.md) [![lang fr](https://img.shields.io/badge/lang-fr-lightgrey)](README.fr.md) [![lang es](https://img.shields.io/badge/lang-es-lightgrey)](README.es.md) [![lang de](https://img.shields.io/badge/lang-de-lightgrey)](README.de.md) [![lang nl](https://img.shields.io/badge/lang-nl-41BDF5)](README.nl.md)

</div>

---

De recorder van Home Assistant bewaart een paar weken historie in SQLite en wordt trager naarmate hij groeit. Scribe schrijft dezelfde toestanden en gebeurtenissen naar **TimescaleDB**, waar jaren snel blijven en een fractie van de ruimte innemen.

- 🚀 **Van begin tot eind asynchroon** — `asyncpg` en gebundelde `COPY`: opnemen blokkeert Home Assistant nooit.
- 🗜️ **Automatisch gecomprimeerd** — oude historie wordt in chunks gedeeld en gecomprimeerd, doorgaans 10× kleiner.
- 🛟 **Er gaat niets verloren** — een database die plat ligt wordt gebufferd en geschreven zodra hij terug is.
- 🧩 **Context inbegrepen** — entiteiten, apparaten, gebieden, gebruikers en integraties, niet alleen waarden.
- 🩺 **Het zegt wanneer er iets mis is**, in Reparaties in plaats van in een log dat niemand leest.

---

## Installatie

**1. Een TimescaleDB-database.** De extensie is verplicht — zie *TimescaleDB opzetten* hieronder.

**2. Scribe, via HACS:**

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=jonathan-gtd&repository=scribe&category=integration)

*Of met de hand:* kopieer `custom_components/scribe` naar uw map `custom_components`. Herstart daarna in beide gevallen Home Assistant.

**3. De database-URL.** Ga naar **Instellingen → Apparaten en diensten → Integratie toevoegen**, zoek **Scribe** en plak hem:

[![Open your Home Assistant instance and start setting up a new integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=scribe)

```
postgresql://scribe:password@192.168.1.10:5432/scribe
```

*Of in `configuration.yaml`*, als u uw configuratie liever in bestanden houdt:

```yaml
scribe:
  db_url: "postgresql://scribe:password@192.168.1.10:5432/scribe"
```

Klaar — toestanden worden opgenomen, in chunks gedeeld en gecomprimeerd, met hun entiteit-, apparaat- en gebiedscontext. Al het onderstaande is optioneel.

---

<details>
<summary><b>🧩 Scribe Card — grafieken op uw dashboard</b></summary>
<br>

**[Scribe Card](https://github.com/jonathan-gtd/scribe-card)** zet elke query uit uw historie op een dashboard. Getekend met Apache ECharts — waar de historiegrafieken van Home Assistant zelf ook op draaien — en ingesteld in een formulier, waarin het grafiektype, de eenheid en de assen worden gekozen uit de kolommen die uw query teruggeeft.

Hij praat met Scribe via de dienst `scribe.query`, dus er is **geen tweede databaseverbinding om in te stellen en geen wachtwoord in uw dashboard**. Installeer hem via HACS als aangepaste repository, categorie *Dashboard*.

</details>

<details>
<summary><b>🗄️ TimescaleDB opzetten</b></summary>
<br>

U hebt een draaiende TimescaleDB-instantie nodig. Ik raad PostgreSQL 17 of 18 aan.

> **❗ Belangrijk** — **De TimescaleDB-extensie is verplicht.** Chunking, compressie, aanbewaring
> en de groottesensoren zijn de hele reden van Scribe, en geen daarvan bestaat op
> kale PostgreSQL. Een nieuwe installatie wordt geweigerd als de extensie ontbreekt —
> al schakelt Scribe hem voor u in wanneer de server hem beschikbaar heeft en uw
> databasegebruiker `CREATE` op de database heeft, wat de opzet hieronder toekent.
> Installaties die al zonder draaien blijven opnemen en horen via een Reparatie
> wat ze missen.

#### Optie A: Home Assistant OS (add-on)

Draait u Home Assistant OS, dan raad ik de [TimescaleDB-add-on](https://github.com/expaso/hassos-addon-timescaledb) aan.

[![Open your Home Assistant instance and show the add add-on repository dialog with a specific repository URL pre-filled.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Fexpaso%2Fhassos-addon-timescaledb)

#### Optie B: Docker (handmatig)

```bash
# High Availability (aanbevolen)
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg18

# Standaard
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:pg18
```

Maak de database en de gebruiker aan:

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
<summary><b>⚙️ Alle opties, met hun standaardwaarden</b></summary>
<br>

### Volledige configuratie (standaardwaarden)

```yaml
scribe:
  # De enige verplichte optie.
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe

  # Al het overige is optioneel. Dit zijn de standaardwaarden.

  # Waar het schrijft
  db_schema: ""                 # leeg = het schema van de verbinding, normaal public
  db_ssl: false                 # TLS naar de database
  ssl_root_cert: ""             # CA-certificaat; alleen gelezen als db_ssl true is
  ssl_cert_file: ""             # clientcertificaat, voor wederzijdse TLS
  ssl_key_file: ""              # de bijbehorende privésleutel

  # Wat het opneemt
  record_states: true           # toestandswijzigingen
  record_events: false          # Home Assistant-gebeurtenissen (automatiseringen, scripts…)
  include_domains: []           # leeg = alle domeinen
  include_entities: []          # leeg = alle entiteiten
  include_entity_globs: []      # bijv. sensor.weer_*
  exclude_domains: []           # wordt na de include-lijsten toegepast
  exclude_entities: []
  exclude_entity_globs: []
  exclude_attributes: []        # attributen die uit de kolom attributes worden gehaald
  include_events: []            # leeg = alle gebeurtenistypen
  exclude_events: []            # wordt na include_events toegepast

  # Hoe lang het ze bewaart
  chunk_time_interval: "7 days" # hoeveel tijd één chunk beslaat
  compress_after: "7 days"      # chunks ouder dan dit worden gecomprimeerd
  retention_states: ""          # leeg = voor altijd; anders VERWIJDERT het oudere toestanden
  retention_events: ""          # leeg = voor altijd; anders VERWIJDERT het oudere gebeurtenissen
  enable_rollups: false         # uur- en dagsamenvattingen van numerieke toestanden

  # Hoe het schrijft
  batch_size: 500               # rijen in de buffer vóór een schrijfactie
  flush_interval: 5             # seconden voordat een onvolledige batch toch wordt geschreven
  max_queue_size: 10000         # rijen in geheugen voordat nieuwe worden weggegooid
  buffer_on_failure: true       # blijven bufferen zolang de database onbereikbaar is

  # Wat scribe.query mag kosten
  query_timeout: 60             # seconden dat een query mag draaien
  query_max_rows: 20000         # rijen die hij mag teruggeven voordat hij wordt geweigerd

  # Sensoren over Scribe zelf
  enable_stats_io: false        # writer-tellers, uit het geheugen gelezen
  enable_stats_chunk: false     # aantal chunks, één query per verversing
  enable_stats_size: false      # schijfgrootte, één query per verversing
  stats_io_interval: 60         # seconden tussen twee schrijfwaarden
  stats_chunk_interval: 60      # minuten tussen twee chunk-queries
  stats_size_interval: 60       # minuten tussen twee grootte-queries

  # Contexttabellen, synchroon met de registers van Home Assistant
  enable_table_areas: true
  enable_table_devices: true
  enable_table_integrations: true
  enable_table_users: true
```

</details>

<details>
<summary><b>📋 Parameteroverzicht</b></summary>
<br>

| Parameter | Beschrijving |
| :--- | :--- |
| `db_url` | **Verplicht.** De verbindingsstring voor uw TimescaleDB-database. |
| `db_ssl` | SSL/TLS inschakelen voor de databaseverbinding. |
| `ssl_root_cert` | Pad naar het CA-certificaat (bijv. `/ssl/ca.crt`). Een relatief pad gaat uit van de configuratiemap van Home Assistant. |
| `ssl_cert_file` | Pad naar het clientcertificaat, voor wederzijdse TLS. |
| `ssl_key_file` | Pad naar de privésleutel van de client, voor wederzijdse TLS. |
| `db_schema` | PostgreSQL-schema om in te schrijven. Leeg (standaard) gebruikt het schema van de verbinding zelf, normaal `public`. |
| `chunk_time_interval` | Hoeveel tijd elke chunk van de tabel beslaat. Zie *Opslagafstemming* hieronder. |
| `compress_after` | Chunks ouder dan dit worden gecomprimeerd. Zie *Opslagafstemming* hieronder. |
| `retention_states` | **Verwijdert** toestandshistorie ouder dan dit interval (bijv. `"365 days"`). Leeg (standaard) bewaart alles. Zie *Aanbewaring* hieronder. |
| `retention_events` | **Verwijdert** gebeurtenishistorie ouder dan dit interval. Leeg (standaard) bewaart alles. Zie *Aanbewaring* hieronder. |
| `record_states` | Of toestandswijzigingen worden opgenomen. |
| `record_events` | Of gebeurtenissen worden opgenomen. |
| `batch_size` | Aantal items dat wordt gebufferd voordat naar de database wordt geschreven. |
| `flush_interval` | Maximale tijd (in seconden) om te wachten voordat de buffer wordt weggeschreven. |
| `max_queue_size` | Maximaal aantal items in geheugen voordat nieuwe worden weggegooid. |
| `query_timeout` | Seconden dat een `scribe.query`-aanroep mag draaien voordat de database hem stopt (standaard `60`). |
| `query_max_rows` | Rijen die een `scribe.query`-aanroep mag teruggeven voordat hij wordt geweigerd (standaard `20000`). |
| `buffer_on_failure` | Zo ja, houdt data in geheugen als de database onbereikbaar is (tot `max_queue_size`). |
| `enable_stats_io` | Realtime prestatiesensoren van de writer inschakelen (geen database-queries). |
| `enable_stats_chunk` | Sensoren voor het aantal chunks inschakelen (bevraagt de database). |
| `enable_stats_size` | Sensoren voor opslaggrootte inschakelen (bevraagt de database). |
| `stats_io_interval` | Seconden tussen twee waarden van de schrijfsensoren (standaard `60`). Elke wijziging is een rij die Scribe over zichzelf opneemt. |
| `stats_chunk_interval` | Interval (in minuten) om de chunkstatistieken bij te werken. |
| `stats_size_interval` | Interval (in minuten) om de groottestatistieken bij te werken. |
| `include_domains` | Lijst van domeinen om op te nemen. |
| `include_entities` | Lijst van specifieke entiteiten om op te nemen. |
| `include_entity_globs` | Lijst van entiteitspatronen om op te nemen (bijv. `sensor.weer_*`). |
| `exclude_domains` | Lijst van domeinen om uit te sluiten. |
| `exclude_entities` | Lijst van specifieke entiteiten om uit te sluiten. |
| `exclude_entity_globs` | Lijst van entiteitspatronen om uit te sluiten (bijv. `switch.keuken_*`). |
| `exclude_attributes` | Lijst van attributen om uit de kolom `attributes` te weren. |
| `include_events` | Lijst van gebeurtenistypen om op te nemen. Leeg laten neemt alle gebeurtenissen op. |
| `exclude_events` | Lijst van gebeurtenistypen die nooit worden opgenomen (na `include_events` toegepast). |
| `enable_table_areas` | Aanmaken en synchroniseren van de tabel `areas` inschakelen. |
| `enable_table_devices` | Aanmaken en synchroniseren van de tabel `devices` inschakelen. |
| `enable_table_integrations` | Aanmaken en synchroniseren van de tabel `integrations` inschakelen. |
| `enable_table_users` | Aanmaken en synchroniseren van de tabel `users` inschakelen. |
| `enable_rollups` | Voorberekende uur- en dagsamenvattingen van toestanden bijhouden (`states_hourly`, `states_daily`). Standaard uit. |

</details>

<details>
<summary><b>🗜️ Opslagafstemming — chunks en compressie</b></summary>
<br>

Scribe bewaart historie in **hypertables** van TimescaleDB: een tabel die eruitziet
en bevraagd wordt als elke andere, maar fysiek is opgesplitst in **chunks**, elk
voor een plak tijd. Vrijwel alles aan Scribes schijfgebruik en querysnelheid komt
neer op die opsplitsing — een query over vorige week leest alleen de chunks die
vorige week overlappen, compressie werkt chunk voor chunk, en *Aanbewaring*
hieronder verwijdert hele chunks in plaats van losse rijen.

Twee instellingen bepalen dat, zowel in YAML als in de interface onder
**Configureren → Geavanceerd (TimescaleDB & SSL)**:

### `chunk_time_interval` (standaard `7 days`)

Hoeveel tijd één chunk beslaat.

- **Kleinere chunks** (bijv. `1 day`) betekenen meer, kleinere bestanden: fijnmaziger
  aanbewaring, en queries over korte recente vensters raken minder data. Voorbij een
  bepaald punt moet een query over maanden honderden chunks openen.
- **Grotere chunks** (bijv. `30 days`) betekenen minder, grotere bestanden: beter voor
  lange historische queries, slechter voor geheugen — TimescaleDB adviseert zelf dat
  de chunks waarin u schrijft samen met hun indexen comfortabel in het geheugen
  passen, dus een te grote chunk op een kleine machine schaadt de schrijfprestaties.

De standaard past bij een typische Home Assistant-installatie. Overweeg `1 day` als u
duizenden entiteiten opneemt, en pas dan.

> **Wijzigen raakt alleen nieuwe chunks.** Reeds geschreven chunks houden de spanne
> waarmee ze zijn gemaakt, en er wordt niets herschreven of verplaatst — u krijgt
> simpelweg een mengsel van oude en nieuwe spannes, waar TimescaleDB van nature mee omgaat.

### `compress_after` (standaard `7 days`)

Hoe oud een chunk moet zijn voordat TimescaleDB hem comprimeert. Compressie levert bij
dit soort data doorgaans een forse verkleining op (veel herhaalde `entity_id`'s en
langzaam veranderende waarden), en staat daarom standaard aan.

Gecomprimeerde chunks blijven volledig bevraagbaar — de view `states` merkt er niets van.
Erin *schrijven* is trager, en daarom slaat compressie pas toe zodra een chunk oud genoeg
is om praktisch af te zijn. Houd `compress_after` ruim boven de leeftijd van de data waar
u nog naartoe schrijft; toestanden die buiten volgorde binnenkomen (een backfill, een
migratiescript) landen in oude chunks.

> **Wijzigen gaat in bij de volgende herstart**, en al gecomprimeerde chunks blijven
> gecomprimeerd — de instelling bepaalt alleen wanneer de *volgende* dat worden.

### Hoe de drie instellingen samenhangen

| Instelling | Wat het doet | Omkeerbaar |
| :--- | :--- | :--- |
| `chunk_time_interval` | Hoeveel tijd één chunk beslaat | Ja — alleen toekomstige chunks |
| `compress_after` | Wanneer een chunk gecomprimeerd wordt | Ja |
| `retention_states` / `retention_events` | Wanneer een chunk **verwijderd** wordt | **Nee** |

Ze gelden in die volgorde voor dezelfde chunk gedurende zijn leven: geschreven →
gecomprimeerd → weggegooid. Twee gevolgen zijn het waard te weten:

- Is `compress_after` groter dan uw aanbewaring, dan worden chunks verwijderd voordat ze
  ooit gecomprimeerd zijn, en doet compressie niets.
- Aanbewaring verwijdert hele chunks, dus uw werkelijke bewaarvenster is het ingestelde
  interval *plus* maximaal één `chunk_time_interval`. Kleinere chunks maken het strakker.

Staan de sensoren voor grootte en chunks aan (`enable_stats_size`, `enable_stats_chunk`),
dan rapporteren zij precies wat deze instellingen opleveren: aantallen chunks,
gecomprimeerde en ongecomprimeerde groottes, en de compressieverhouding.

</details>

<details>
<summary><b>🧹 Aanbewaring — oude historie volgens schema weggooien</b></summary>
<br>

Standaard bewaart Scribe alles, voor altijd. Wilt u slechts een begrensd venster opslaan
— omdat u de ruwe historie elders samenvat, of eenvoudigweg om het schijfgebruik af te
toppen — stel dan een bewaarinterval in, en TimescaleDB gooit chunks weg die ouder zijn:

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  retention_states: "365 days"
  retention_events: "30 days"
```

Beide staan ook in de interface onder **Configureren → Geavanceerd (TimescaleDB & SSL)**.

> **⚠️ Let op** — Aanbewaring **verwijdert data definitief**. Er is geen ongedaan maken en geen
> prullenbak: zodra een chunk buiten het venster valt wordt hij weggegooid, en alleen een
> back-up brengt hem terug. Toestanden en gebeurtenissen worden apart ingesteld, zodat u
> luidruchtige gebeurtenissen kunt laten verlopen terwijl u de toestandshistorie houdt.

Het waard om te weten:

- **Geen instelling betekent altijd "voor altijd bewaren".** Het veld in de interface legen
  en de regel uit `configuration.yaml` halen verwijderen allebei het beleid — een waarde die
  Scribe ooit uit YAML overnam mag nooit de regel overleven die hem zette.
- **Scribe is eigenaar van het bewaarbeleid op zijn eigen tabellen.** Het veld legen verwijdert
  het beleid — ook een dat u met de hand maakte via `add_retention_policy()`, wat de enige
  manier is waarop het legen van de instelling de verwijderingen echt kan stoppen.
- **Het begint meteen.** TimescaleDB draait het beleid binnen seconden na het aanmaken, niet
  bij het volgende dagelijkse interval — alles buiten het venster is weg bij de eerste run,
  vlak na de herstart die het inschakelde.
- **Verwijderen gaat per chunk, niet per rij.** Een chunk wordt pas weggegooid als *alle* rijen
  erin ouder zijn dan het interval, dus met de standaard `chunk_time_interval` van 7 dagen
  houdt u tot een week meer dan u vroeg. Juist dat maakt aanbewaring bijna gratis: het gooit
  bestanden weg in plaats van rijen te verwijderen.
- **Alleen de historie wordt verwijderd.** De tabel `entities` en de andere metadatatabellen
  worden niet aangeraakt, dus een entiteit waarvan de historie volledig verlopen is, blijft
  oplosbaar.
- **TimescaleDB is vereist** — het is de extensie die het beleid uitvoert. Op kale PostgreSQL
  levert een bewaarinterval instellen een Reparatie op in plaats van stilletjes niets te doen.
- Toegestane waarden zijn gewone intervallen: `30 days`, `6 months`, `1 year`. Al het andere
  wordt met een foutmelding geweigerd in plaats van naar de database gestuurd.

</details>

<details>
<summary><b>📈 Samenvattingen — uur- en dagaggregaten</b></summary>
<br>

Een jaar van een sensor die elke 30 seconden meldt, is ongeveer een miljoen rijen. Een grafiek van dat jaar leest ze allemaal, elke keer dat hij wordt getekend.

Met `enable_rollups: true` houdt TimescaleDB twee samenvattingen van uw toestanden bij terwijl ze worden geschreven — per uur en per dag — en leest een grafiek over jaren duizenden rijen in plaats van miljoenen.

```yaml
scribe:
  enable_rollups: true
```

Het staat ook in de interface onder **Configureren → Metadatatabellen**. Dat voegt twee views toe:

| View | Eén rij per | Kolommen |
| --- | --- | --- |
| `states_hourly` | entiteit en uur | `entity_id`, `bucket`, `value_avg`, `value_min`, `value_max`, `samples` |
| `states_daily` | entiteit en dag | dezelfde |

```sql
SELECT bucket, value_avg, value_min, value_max
FROM states_daily
WHERE entity_id = 'sensor.buitentemperatuur'
  AND bucket > now() - interval '2 years'
ORDER BY bucket;
```

Alleen numerieke toestanden worden samengevat — het gemiddelde van `on` en `off` betekent niets — dus `value_avg`, `value_min` en `value_max` blijven leeg voor de rest, terwijl `samples` elke toestand in de bucket telt.

**Het zijn afgeleide gegevens.** Er wordt niets gedupliceerd dat u zou missen: de optie uitzetten verwijdert beide views, hem weer aanzetten bouwt ze opnieuw op uit de historie, en uw toestanden worden hoe dan ook nooit aangeraakt. TimescaleDB ververst ze zelf — die per uur elke 30 minuten, die per dag elk uur — en elke run kijkt ver genoeg terug (3 dagen, 30 dagen) dat een laat geschreven batch er alsnog in landt. Scribe maakt ze alleen aan.

</details>

<details>
<summary><b>🗃️ Naar een specifiek PostgreSQL-schema schrijven</b></summary>
<br>

Standaard schrijft Scribe naar het schema waar uw verbinding toch al naar wijst — normaal `public`. Zet `db_schema` en het maakt dat schema aan en zet er alles in: zijn tabellen, zijn views, zijn hypertables en zijn beleidsregels.

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  db_schema: scribe
```

Het staat ook in de interface onder **Configureren → Geavanceerd (TimescaleDB & SSL)**.

Dit wilt u wanneer Scribe een database deelt met iets anders: de tabellen van een andere integratie, uw eigen kopieën van de historie, of een tweede Home Assistant die naar dezelfde server schrijft. Schema's zijn onafhankelijk — eigen tabellen, hypertables, aanbewaring en compressie — en niets van wat Scribe in het ene doet, raakt het andere.

- **Alleen nieuwe data gaat erheen.** `db_schema` zetten verplaatst reeds opgenomen historie niet. Verplaats die zelf vóór de herstart (`ALTER TABLE public.states_raw SET SCHEMA scribe;`), of bevraag het oude schema rechtstreeks.
- **Scribe maakt het schema aan als het mag**, waarvoor `CREATE` op de database nodig is. Een schema dat u met de hand maakte werkt ook, mits `USAGE` en `CREATE` erop.
- **Een onbereikbaar schema stopt het opnemen.** PostgreSQL valt terug op het volgende item van het search path in plaats van te falen, dus een typefout zou anders `public` vullen terwijl de interface iets anders toont. Scribe controleert waar het geland is en schrijft liever niets dan op de verkeerde plek, met een Reparatie die zegt wat er toegekend moet worden.
- **Uw queries veranderen niet.** Scribe zet het schema vooraan in het `search_path` van de verbinding, dus `SELECT * FROM states` blijft werken via `scribe.query`. Vanuit Grafana of psql kwalificeert u de naam (`scribe.states`) of zet u uw eigen `search_path`. `public` blijft op het pad — daar wonen de TimescaleDB-functies.
- Toegestane waarden zijn gewone identifiers: letters, cijfers en underscores, niet beginnend met een cijfer. Leeg behoudt het schema van de verbinding zelf, ook een dat u zelf met `?options=-csearch_path%3Dmijnschema` in de URL hebt gezet.

**De tabellen zelf** — elke kolom, hoe ze samenhangen, en queryrecepten voor Grafana en `scribe.query` — staan beschreven in [`docs/data-structure.md`](docs/data-structure.md).

</details>

<details>
<summary><b>🛠️ Diensten — flush, query, purge</b></summary>
<br>

### `scribe.flush`
Forceer het onmiddellijk wegschrijven van gebufferde data naar de database.

```yaml
service: scribe.flush
```

### `scribe.query`
Voer een alleen-lezen SQL-query uit op de TimescaleDB-database.

**Parameters:**
- `sql` (verplicht): de uit te voeren SQL-query. Moet een `SELECT` zijn.

**Geeft terug:**
Een lijst met rijen, waarbij elke rij een woordenboek is van kolomnamen en waarden.

**Voorbeeld:**
```yaml
service: scribe.query
data:
  sql: "SELECT * FROM states ORDER BY time DESC LIMIT 5"
response_variable: query_result
```

### `scribe.purge`
Verwijder opgenomen historie. **Dit kan niet ongedaan worden gemaakt.**

**Parameters** (ten minste een van de eerste twee is verplicht):
- `entity_id`: entiteiten om op te schonen. Zonder `keep_days` worden hun hele historie *en* hun rij in de tabel `entities` verwijderd — ze opnieuw opnemen begint dan vanaf niets.
- `keep_days`: verwijder alles ouder dan dit aantal dagen.
- `events` (standaard `false`): verwijder ook gebeurtenissen ouder dan `keep_days`. Zonder dat genegeerd.

**Geeft terug:** hoeveel toestanden, gebeurtenissen en entiteitsrijen zijn verwijderd.

**Voorbeelden:**
```yaml
# Eén entiteit volledig uit de database halen
action: scribe.purge
data:
  entity_id: sensor.sensor_die_ik_niet_meer_wil
```

```yaml
# Alles ouder dan twee jaar inkorten, gebeurtenissen inbegrepen
action: scribe.purge
data:
  keep_days: 730
  events: true
response_variable: purged
```

Gecomprimeerde historie wordt ook opgeschoond; TimescaleDB regelt dat, en de chunks blijven gecomprimeerd. Voor een doorlopend venster dat u wilt blijven toepassen, gebruikt u in plaats daarvan de instellingen onder *Aanbewaring* hieronder: een purge is eenmalig.

</details>

<details>
<summary><b>📊 Statistieksensoren</b></summary>
<br>

Sensoren schakelt u in met hun vlaggen in uw configuratie.

### Schrijfstatistieken (`enable_stats_io: true`)

Realtime metingen uit de writer (geen database-queries).

| Sensor | Beschrijving |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_states_written` | Totaal aantal toestandswijzigingen dat naar de database is geschreven. |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_events_written` | Totaal aantal gebeurtenissen dat naar de database is geschreven. |
| <img src="https://api.iconify.design/mdi:buffer.svg?color=%232196F3" width="15" /> `sensor.scribe_buffer_size` | Huidig aantal items dat in de geheugenbuffer wacht. |
| <img src="https://api.iconify.design/mdi:timer-sand.svg?color=%232196F3" width="15" /> `sensor.scribe_last_write_duration` | Duur (in ms) van de laatste schrijfactie naar de database. |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_states_rate` | Snelheid van naar de database geschreven toestanden (per minuut). |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_events_rate` | Snelheid van naar de database geschreven gebeurtenissen (per minuut). |

### Chunkstatistieken (`enable_stats_chunk: true`)

Aantallen chunks (bijgewerkt elke `stats_chunk_interval` minuten).

| Sensor | Beschrijving |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_chunks` | Totaal aantal chunks voor de tabel states. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_chunks` | Aantal chunks dat gecomprimeerd is. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_chunks` | Aantal chunks dat op compressie wacht. |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_chunks` | Totaal aantal chunks voor de tabel events. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_chunks` | Aantal gecomprimeerde gebeurtenis-chunks. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_chunks` | Aantal ongecomprimeerde gebeurtenis-chunks. |

### Groottestatistieken (`enable_stats_size: true`)

Opslaggebruik in bytes (bijgewerkt elke `stats_size_interval` minuten).

| Sensor | Beschrijving |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_size` | Totale schijfgrootte (inclusief gecomprimeerde data + recente chunks + indexen). |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_states_original_size` | **Theoretische grootte** als de data niet gecomprimeerd was (bijv. 11 GB). |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_size` | Fysieke grootte van de gecomprimeerde datachunks. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_size` | Grootte van recente data die nog niet gecomprimeerd is (of indexen in afwachting). |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compression_ratio` | Compressieverhouding voor toestanden (%). |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_size` | Totale schijfgrootte van de tabel events. |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_events_original_size` | Theoretische grootte van gebeurtenissen vóór compressie. |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_size` | Grootte van gecomprimeerde gebeurtenisdata. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_size` | Grootte van ongecomprimeerde gebeurtenisdata. |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compression_ratio` | Compressieverhouding voor gebeurtenissen (%). |

</details>

<details>
<summary><b>🖼️ Dashboard</b></summary>
<br>

Een kant-en-klare Lovelace-indeling met alle nuttige Scribe-sensoren (databasestatistieken, compressieverhoudingen, schrijfprestaties) staat in deze repository, in twee smaken:

| Bestand | Wat het is | Waar u het plakt |
| --- | --- | --- |
| [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml) | Eén **enkele kaart** (`type: vertical-stack`) | De YAML-editor van de kaart ("Kaart toevoegen" → "Handmatig") |
| [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml) | Een **hele weergave** (`title` / `icon` / `cards`) | De YAML-editor van de weergave |

> ⚠️ Deze twee zijn niet uitwisselbaar. Het *weergave*-bestand in een *kaart*-editor plakken mislukt met **"No card type configured"**, omdat een kaartconfiguratie met een `type:`-sleutel moet beginnen.

**Optie A — als kaart toevoegen (eenvoudigst, werkt in elk weergavetype):**

1.  Open uw dashboard en klik op "Dashboard bewerken" (potloodpictogram).
2.  Klik op **+ Kaart toevoegen** en scroll naar de onderkant van de kaartkiezer om **Handmatig** te kiezen.
3.  Kopieer de inhoud van [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml), vervang alles in de editor ermee, en klik op **Opslaan**.

**Optie B — als eigen weergave toevoegen:**

1.  Open uw dashboard en klik op "Dashboard bewerken" (potloodpictogram).
2.  Klik op de knop **+** *in de tabbalk bovenaan* (naast uw bestaande weergavenamen) om een nieuwe weergave toe te voegen — niet op "Kaart toevoegen".
3.  Open in het weergavevenster het menu ⋮ (of de knop "Code-editor weergeven") en kies **In YAML bewerken**.
4.  Kopieer de inhoud van [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml), vervang alles in de editor ermee, en klik op **Opslaan**.

</details>

<details>
<summary><b>📦 Migreren vanaf InfluxDB, LTSS, de recorder of Scribe 2.x</b></summary>
<br>

### Bijwerken vanaf Scribe 2.x

Scribe 3.0 verving de tabel `states` door `states_raw` plus een compatibiliteitsview, en
gaf `entities` een numerieke primaire sleutel. De omzetting van een oude database werd
door 3.x gedaan en is **in 3.9 verwijderd**.

Heeft uw database nog een `states`-*tabel* (in plaats van een view), een tabel
`states_legacy`, of een tabel `entities` zonder kolom `id`, dan stopt Scribe bij het
starten, neemt niets op en meldt een Reparatie — zonder iets te hernoemen, aan te maken
of te verwijderen. Installeer **Scribe 3.8**, laat Home Assistant draaien tot de logs
melden dat de migratie klaar is (ongeveer een kwartier op een grote database), en werk
daarna opnieuw bij.

Nieuwe installaties en elke door 3.x gemaakte database hebben hier geen last van.

### Data uit andere bronnen overnemen

In `migration/` staan drie scripts die historie van elders naar Scribe kopiëren. Ze worden eenmalig met de hand gedraaid, vanaf een machine die beide databases bereikt — en Scribe moet minstens één keer gestart zijn, zodat zijn tabellen bestaan.

```bash
cd migration
pip install psycopg2-binary python-dotenv   # voor InfluxDB daarnaast influxdb-client
cp .env.example .env && nano .env
python3 <script>.py
```

| Bron | Script | In te vullen |
| --- | --- | --- |
| InfluxDB | `influx2scribe.py` | `INFLUX_*` |
| LTSS | `ltss2scribe.py` | `LTSS_*` |
| Home Assistant-recorder | `recorder2scribe.py` | `RECORDER_*`, met `RECORDER_TYPE` op `postgres` of `sqlite` (SQLite heeft alleen `RECORDER_DB_PATH` nodig) |

Elke run heeft daarnaast `SCRIBE_*` nodig — de bestemming — en de migratie-instellingen: `MIGRATION_START_TIME`, `MIGRATION_END_TIME`, `CHUNK_SIZE` (uren per batch) en `PURGE_DESTINATION`, dat **de historie van de bestemming wist vóór het importeren**. Laat het op `False` staan tenzij u het echt bedoelt.

Elk script controleert het bestemmingsschema voordat het iets schrijft, en stopt met een uitleg in plaats van een muur aan fouten rij voor rij als Scribe het nooit heeft geïnitialiseerd.

</details>

<details>
<summary><b>🩺 Probleemoplossing</b></summary>
<br>

### Eerst dit

Twee plekken beantwoorden "waarom wordt er niets opgenomen?" zonder ook maar één logregel te lezen:

- **Instellingen → Apparaten en diensten → Scribe → ⋮ → Diagnose downloaden** meldt wat de
  writer werkelijk doet: verbonden of niet, of TimescaleDB gevonden is, hoeveel items in de
  buffer wachten en hoeveel er zijn weggegooid, opeenvolgende schrijffouten, en de geldende
  opslag- en bewaarinstellingen. De database-URL zit er nooit in, en uit driverfouten wordt
  elke verbindingsstring gestript.
- **Instellingen → Systeem → Reparaties** somt de onderstaande situaties op, en
  **Instellingen → Systeem → Systeemstatus** toont op welke database Scribe is gericht en of
  hij op dit moment verbonden is.

### Reparaties

Scribe meldt problemen die het niet zelf kan oplossen in **Instellingen → Systeem → Reparaties**, zodat u de logs niet hoeft te bewaken. Elke melding verdwijnt vanzelf zodra de situatie is opgelost.

| Reparatie | Wat het betekent |
| --- | --- |
| Kan de database niet bereiken | De verbinding is mislukt. Scribe blijft bufferen en probeert het op de achtergrond opnieuw, dus historie uit de storing wordt geschreven zodra de database terug is. Controleer of de server draait en of URL en inloggegevens kloppen. |
| Kan niet naar de database schrijven | Meerdere schrijfacties achter elkaar zijn mislukt. Data staat in geheugen en wordt bij herstel weggeschreven — tenzij Home Assistant eerst herstart. |
| Buffer is vol | De schrijfacties mislukten lang genoeg om de buffer te verzadigen; de oudste records worden nu weggegooid. Herstel de database, of verhoog `max_queue_size`. |
| Records worden weggegooid | Een schrijfactie mislukte terwijl bufferen uitstaat, dus records zijn meteen weggegooid. Zet bufferen aan om korte storingen te overleven. |
| Kon zijn tabellen niet aanmaken | Scribe bereikte de database maar kon zijn schema niet bouwen, meestal een rechtenprobleem. Op een nieuwe database wordt er helemaal niets opgenomen. |
| Kan het opgegeven schema niet bereiken | Het schema in `db_schema` bestaat niet en kon niet worden aangemaakt, of de databasegebruiker heeft er geen rechten op. Er wordt niets opgenomen — in plaats van stilletjes `public` te vullen. |
| Kon de view `states` niet aanmaken | Historie wordt opgenomen, maar de view waar elke query doorheen gaat ontbreekt — de historie lijkt leeg terwijl er niets verloren is. |
| `states_raw` / `events` is geen hypertable | TimescaleDB is geïnstalleerd maar de tabel is nooit omgezet (gebruikelijk wanneer de extensie *na* het vollopen van de tabellen is toegevoegd). Chunking, compressie en aanbewaring doen dan niets. |
| `states_raw` / `events` wordt nooit gecomprimeerd | De tabel is een hypertable maar heeft geen compressiebeleid, en houdt dus zijn volle ongecomprimeerde grootte. |
| TLS geldt niet volledig | Scribe verbindt via TLS, maar een door u ingesteld certificaat kon niet worden toegepast — meestal een clientcertificaat, waardoor het zich als gewone client aanmeldt in plaats van als degene die u voorzag. |
| TimescaleDB is niet geïnstalleerd | Historie wordt opgenomen, maar chunking en compressie zijn niet beschikbaar, dus de database groeit veel sneller en de groottesensoren blijven leeg. |
| Database dateert van vóór versie 3.0 | De database gebruikt nog de indeling van vóór 3.0, die deze versie niet kan omzetten. Er wordt niets opgenomen en er is niets gewijzigd — installeer Scribe 3.8 om hem om te zetten, en werk daarna opnieuw bij. |
| Kon het bewaarbeleid niet toepassen | U vroeg Scribe data ouder dan een interval te verwijderen en het beleid kon niet worden aangemaakt. Er is niets verwijderd, en er wordt niets verwijderd — de tabel blijft groeien. |
| Hernoemen van entiteit niet toegepast | Een hernoeming botste met een bestaande rij in de database. De historie van de entiteit staat verdeeld over de twee ID's. |

### Hoog geheugengebruik
- Verlaag `max_queue_size`
- Verlaag `flush_interval` voor sneller schrijven
- Controleer `sensor.scribe_buffer_size`

### Prestatieafstemming

Is de view `states` traag (meerdere seconden per query), dan komt dat waarschijnlijk doordat de queryplanner van PostgreSQL een **Hash Join** kiest in plaats van een **Nested Loop**, waardoor TimescaleDB chunks niet effectief kan wegsnoeien.

De meest voorkomende oorzaak is een hoge `random_page_cost` (standaard `4.0`, geoptimaliseerd voor harde schijven). Gebruikt u moderne opslag (SSD, NVMe) of is uw database goed gecachet, verlaag die waarde dan:

```sql
-- Huidige waarde bekijken
SHOW random_page_cost;

-- Op een lagere waarde zetten (meestal 1.1)
ALTER SYSTEM SET random_page_cost = 1.1;
SELECT pg_reload_conf();
```

Die waarde verlagen moedigt de planner aan om index-gebaseerde joins (Nested Loops) te gebruiken, die essentieel zijn voor Scribes prestaties bij grote datasets.

### Nog steeds problemen?
[Open een issue](https://github.com/jonathan-gtd/scribe/issues) op GitHub met uw logs en configuratie. Ik help graag!

</details>

---

## Licentie

MIT-licentie — zie het bestand LICENSE voor details

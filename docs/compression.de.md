# Die Kompression, gemessen

[← README](../README.de.md) · abgelesen am 2026-10-19

Kein Benchmark. Eine Home-Assistant-Instanz, die Datenbank hinter diesem README,
mit Scribes Standardeinstellungen: Chunks von 7 Tagen, komprimiert, sobald sie
sieben Tage alt sind, nichts nachjustiert.

| | |
| :--- | :--- |
| Historie | 53 147 521 Zustände von 1 948 Entitäten, seit dem 2024-08-19 |
| Pro Tag geschrieben | 48 902 Zustände, 0,57 pro Sekunde |
| Chunks | 114, davon 112 komprimiert |
| Vor der Kompression | 18 GB |
| Nach der Kompression | 608 MB |
| **Verhältnis** | **29,7×** |
| Hypertable auf der Platte | **733 MB**, die zwei unkomprimierten Chunks eingerechnet |

TimescaleDB misst es selbst: der Wert lässt sich auf jeder Scribe-Datenbank
nachstellen, statt zitiert zu werden.

```sql
SELECT pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after,
       round(before_compression_total_bytes::numeric
             / after_compression_total_bytes, 1)      AS ratio
FROM hypertable_compression_stats('states_raw');
```

```
 before | after  | ratio
--------+--------+-------
 18 GB  | 608 MB |  29.7
```

## Wohin der Platz geht

Die Summe verbirgt drei verschiedene Geschichten:

| | Vorher | Nachher |
| :--- | ---: | ---: |
| Tabelle | 11 GB | 64 MB |
| Indizes | 6867 MB | 6760 kB |
| TOAST | 960 kB | 538 MB |
| **Gesamt** | **18 GB** | **608 MB** |

- **Die Tabelle** trägt die Zustände selbst: aus 11 GB werden 64 MB
  Chunk-Gerüst — die Daten wandern komprimiert ins TOAST.
- **Die Indizes** hören fast auf zu existieren: 6867 MB gegen 6760 kB, etwa
  1000×. Ein komprimierter Chunk braucht keinen B-Baum auf `(metadata_id,
  time)` — er hält Min und Max je Batch und liest den Batch. Dieser Einbruch ist
  ein Drittel der 18 GB und der Grund, warum das Gesamtverhältnis höher ausfällt
  als das jedes einzelnen Chunks.
- **Das TOAST** ist der Ort der komprimierten Batches: 538 MB der 608 MB.

## Pro Zustand

| | Unkomprimiert | Komprimiert |
| :--- | ---: | ---: |
| Bytes pro Zustand, Indizes eingerechnet | 260 B | 10,6 B |
| Ein Tag, bei 48 902 Zuständen | 12,7 MB | 0,5 MB |
| Ein Jahr, bei gleichem Tempo | 4,6 GB | **189 MB** |

An echten Chunks dieser Datenbank gemessen: 84 MB für 336 801 Zustände im
laufenden Chunk, 4200 kB für 404 737 Zustände im zuletzt komprimierten.

## Chunk für Chunk

```sql
SELECT chunk_name,
       pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after,
       round(before_compression_total_bytes::numeric
             / after_compression_total_bytes, 1)      AS ratio
FROM chunk_compression_stats('states_raw')
WHERE after_compression_total_bytes > 0
ORDER BY chunk_name DESC LIMIT 5;
```

```
     chunk_name     | before |  after  | ratio
--------------------+--------+---------+-------
 _hyper_9_497_chunk | 101 MB | 4448 kB |  23.3
 _hyper_9_493_chunk | 88 MB  | 4056 kB |  22.1
 _hyper_9_489_chunk | 86 MB  | 3960 kB |  22.1
 _hyper_9_485_chunk | 89 MB  | 4056 kB |  22.6
 _hyper_9_481_chunk | 93 MB  | 4368 kB |  21.8
```

Über die 112 komprimierten Chunks: **Durchschnitt 28,8×, Median 25,9×, von
13,3× bis 94,7×**. Die Streuung kommt aus der Historie selbst, nicht aus der
Kompression — eine Woche, in der eine Handvoll Entitäten denselben Wert
wiederholen, schrumpft auf fast nichts; eine Woche mit einem Dutzend
geschwätziger Sensoren, die tatsächlich verschiedene Werte melden, nicht. Die
jüngsten Chunks liegen um 22× und damit unter dem Gesamtwert, weil der Einbruch
der Indizes nur einmal zählt, über die ganze Hypertable.

## Warum es so gut komprimiert

Home-Assistant-Historie ist der Idealfall für spaltenweise Kompression, und die
Datenbank sagt das auch. Über 50 000 aufeinanderfolgende Zustände:

```sql
SELECT count(*)                    AS rows,
       count(DISTINCT state)       AS distinct_states,
       count(DISTINCT attributes)  AS distinct_attributes,
       round(avg(pg_column_size(attributes))) AS avg_attr_bytes,
       round(avg(pg_column_size(state)))      AS avg_state_bytes
FROM (SELECT * FROM states_raw
      WHERE time >= now() - interval '2 days' LIMIT 50000) s;
```

```
 rows  | distinct_states | distinct_attributes | avg_attr_bytes | avg_state_bytes
-------+-----------------+---------------------+----------------+-----------------
 50000 |             502 |                2739 |            120 |               8
```

Fünfhundert verschiedene Werte auf fünfzigtausend Zeilen. Der `attributes`-Blob,
im Schnitt 120 Bytes gegen 8 für den Zustand selbst, wiederholt sich fast ebenso
— dieselbe Einheit, dieselbe Device Class, derselbe Anzeigename, in jeder Zeile.
Spaltenweise statt zeilenweise abgelegt, ist das ein Wörterbuch von einigen
hundert Einträgen plus ein Verweis darauf; die streng steigenden Zeitstempel
werden zu Deltas.

Das ist zugleich die ehrliche Grenze der Zahl: eine Instanz, die vor allem
numerische Sensoren aufzeichnet, die sich wirklich ändern, mit durch
`exclude_attributes` entfernten Attributen, komprimiert schlechter. Eine, die
tausend binäre Sensoren aufzeichnet, besser.

## Komprimiert heißt nicht langsamer zu lesen

Im Gegenteil, für die Abfragen, die ein Dashboard stellt. Derselbe
Stundendurchschnitt, über eine komprimierte Woche und über die laufende:

```sql
-- the same query, over a compressed week and over the live one
SELECT time_bucket('1 hour', time) AS bucket, avg(state::float)
FROM states_raw
WHERE metadata_id = 31
  AND time >= '2026-10-01' AND time < '2026-10-08'   -- compressed
  AND state ~ '^[0-9.]+$'
GROUP BY 1 ORDER BY 1;
```

| Week | Chunk | Rows read | Time |
| :--- | :--- | ---: | ---: |
| 10-01 → 10-08 | compressed | 12 991 | **0.97 ms** |
| 10-15 → 10-22 | uncompressed | 8 084 | 6.93 ms |

| Woche | Chunk | Gelesene Zeilen | Zeit |
| :--- | :--- | ---: | ---: |
| 01.10. → 08.10. | komprimiert | 12 991 | **0,97 ms** |
| 15.10. → 22.10. | unkomprimiert | 8 084 | 6,93 ms |

Siebenmal schneller bei 1,6× mehr Zeilen — rund 11× je Zeile. Ein komprimierter
Chunk liest eine Handvoll Batches, statt 13 000 Zeilenversionen zu durchlaufen,
und die Spalten, die die Abfrage nie nennt, werden überhaupt nicht gelesen.

Langsamer ist ein komprimierter Chunk beim Beschriebenwerden. Genau dafür gibt
es `compress_after`: die Kompression von dem Chunk fernhalten, der noch Zustände
aufnimmt. Verspätete Zeilen — ein Backfill, ein Migrationsskript, ein Nachspielen
nach einer nicht erreichbaren Datenbank — landen in einem bereits komprimierten
Chunk, werden in dessen unkomprimierten Teil geschrieben und später neu
komprimiert.

## Aus Home Assistant heraus lesen

Mit `enable_stats_size: true` sind dieselben drei Zahlen Sensoren — *States
Original Size*, *States Compressed Size* und *States Compression Ratio* — alle
`stats_size_interval` Minuten aktualisiert, mit den Chunk-Zählern daneben unter
`enable_stats_chunk`. Ohne SQL und ohne zweite Verbindung zur Datenbank.

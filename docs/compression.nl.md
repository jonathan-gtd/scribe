# Compressie, gemeten

[← README](../README.nl.md) · gelezen op 2026-10-19

Geen benchmark. Eén Home Assistant-instantie, de database achter deze README,
op de standaardinstellingen van Scribe: chunks van 7 dagen, gecomprimeerd zodra
ze zeven dagen oud zijn, niets bijgesteld.

| | |
| :--- | :--- |
| Historie | 53 147 521 toestanden van 1 948 entiteiten, sinds 2024-08-19 |
| Per dag geschreven | 48 902 toestanden, 0,57 per seconde |
| Chunks | 114, waarvan 112 gecomprimeerd |
| Vóór compressie | 18 GB |
| Na compressie | 608 MB |
| **Verhouding** | **29,7×** |
| Hypertable op schijf | **733 MB**, de twee niet-gecomprimeerde chunks meegerekend |

TimescaleDB meet het zelf: het cijfer is na te spelen op elke Scribe-database in
plaats van geciteerd.

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

## Waar de ruimte heen gaat

Het totaal verbergt drie verschillende verhalen:

| | Voor | Na |
| :--- | ---: | ---: |
| Tabel | 11 GB | 64 MB |
| Indexen | 6867 MB | 6760 kB |
| TOAST | 960 kB | 538 MB |
| **Totaal** | **18 GB** | **608 MB** |

- **De tabel** draagt de toestanden zelf: 11 GB wordt 64 MB chunkskelet — de
  gegevens verhuizen gecomprimeerd naar TOAST.
- **De indexen** houden bijna op te bestaan: 6867 MB tegen 6760 kB, ongeveer
  1000×. Een gecomprimeerde chunk heeft geen B-tree op `(metadata_id, time)`
  nodig — hij houdt een min en max per batch bij en leest de batch. Die
  instorting is een derde van de 18 GB, en daarom ligt de totale verhouding
  hoger dan die van welke chunk dan ook afzonderlijk.
- **TOAST** is waar de gecomprimeerde batches wonen: 538 MB van de 608 MB.

## Per toestand

| | Niet gecomprimeerd | Gecomprimeerd |
| :--- | ---: | ---: |
| Bytes per toestand, indexen meegerekend | 260 B | 10,6 B |
| Eén dag, bij 48 902 toestanden | 12,7 MB | 0,5 MB |
| Eén jaar, in hetzelfde tempo | 4,6 GB | **189 MB** |

Gemeten op echte chunks van deze database: 84 MB voor 336 801 toestanden in de
lopende chunk, 4200 kB voor 404 737 toestanden in de laatst gecomprimeerde.

## Chunk voor chunk

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

Over de 112 gecomprimeerde chunks: **gemiddeld 28,8×, mediaan 25,9×, van 13,3×
tot 94,7×**. De spreiding komt uit de historie zelf, niet uit de compressie —
een week waarin een handvol entiteiten dezelfde waarde herhalen krimpt tot bijna
niets; een week met een stuk of tien spraakzame sensoren die werkelijk
verschillende waarden melden niet. De recente chunks zitten rond 22×, onder het
totaalcijfer, omdat de instorting van de indexen hierboven maar één keer telt,
over de hele hypertable.

## Waarom het zo goed comprimeert

Home Assistant-historie is het ideale geval voor kolomsgewijze compressie, en de
database zegt dat ook. Over 50 000 opeenvolgende toestanden:

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

Vijfhonderd verschillende waarden op vijftigduizend rijen. De `attributes`-blob,
gemiddeld 120 bytes tegen 8 voor de toestand zelf, herhaalt zich bijna even
vaak — dezelfde eenheid, dezelfde device class, dezelfde weergavenaam, op elke
rij. In kolommen opgeslagen in plaats van in rijen is dat een woordenboek van
een paar honderd ingangen plus een verwijzing erin; de strikt oplopende
tijdstempels worden delta's.

Dat is meteen de eerlijke grens van het cijfer: een instantie die vooral
numerieke sensoren opneemt die echt veranderen, met attributen weggehaald door
`exclude_attributes`, comprimeert minder. Eén die duizend binaire sensoren
opneemt, meer.

## Gecomprimeerd betekent niet trager te lezen

Integendeel, voor de query's die een dashboard doet. Hetzelfde uurgemiddelde,
over een gecomprimeerde week en over de week die wordt geschreven:

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

| Week | Chunk | Gelezen rijen | Tijd |
| :--- | :--- | ---: | ---: |
| 01-10 → 08-10 | gecomprimeerd | 12 991 | **0,97 ms** |
| 15-10 → 22-10 | niet gecomprimeerd | 8 084 | 6,93 ms |

Zeven keer sneller op 1,6× meer rijen — ongeveer 11× per rij. Een gecomprimeerde
chunk leest een handvol batches in plaats van 13 000 rijversies te doorlopen, en
de kolommen die de query nooit noemt worden helemaal niet gelezen.

Waar een gecomprimeerde chunk wél trager in is, is beschreven worden. Daar gaat
`compress_after` over: de compressie weghouden van de chunk die nog toestanden
ontvangt. Rijen die laat binnenkomen — een backfill, een migratiescript, een
herhaling na een onbereikbare database — belanden in een al gecomprimeerde
chunk, worden in het niet-gecomprimeerde deel geschreven en later opnieuw
gecomprimeerd.

## Het uitlezen vanuit Home Assistant

Met `enable_stats_size: true` zijn diezelfde drie getallen sensoren — *States
Original Size*, *States Compressed Size* en *States Compression Ratio* — elke
`stats_size_interval` minuten ververst, met de chunkaantallen ernaast onder
`enable_stats_chunk`. Zonder SQL, en zonder tweede verbinding met de database.

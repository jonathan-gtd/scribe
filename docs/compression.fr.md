# La compression, mesurée

[← README](../README.fr.md) · relevé le 2026-10-19

Pas un banc d'essai. Une instance Home Assistant, la base derrière ce README,
avec les réglages Scribe par défaut : des chunks de 7 jours, compressés dès
qu'ils ont sept jours, rien d'ajusté.

| | |
| :--- | :--- |
| Historique | 53 147 521 états sur 1 948 entités, depuis le 2024-08-19 |
| Écrit par jour | 48 902 états, 0,57 par seconde |
| Chunks | 114, dont 112 compressés |
| Avant compression | 18 Go |
| Après compression | 608 Mo |
| **Ratio** | **29,7×** |
| Hypertable sur le disque | **733 Mo**, les deux chunks non compressés compris |

TimescaleDB le mesure lui-même : le chiffre se rejoue sur n'importe quelle base
Scribe au lieu d'être cité.

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

## Où part la place

Le total recouvre trois histoires différentes :

| | Avant | Après |
| :--- | ---: | ---: |
| Table | 11 Go | 64 Mo |
| Index | 6867 Mo | 6760 ko |
| TOAST | 960 ko | 538 Mo |
| **Total** | **18 Go** | **608 Mo** |

- **La table** porte les états eux-mêmes : 11 Go deviennent 64 Mo de squelette
  de chunk — les données partent dans le TOAST, compressées.
- **Les index** cessent presque d'exister : 6867 Mo contre 6760 ko, environ
  1000×. Un chunk compressé n'a pas besoin d'un B-tree sur `(metadata_id, time)`
  — il garde un min/max par lot et lit le lot. Cet effondrement représente un
  tiers des 18 Go, et c'est pourquoi le ratio global dépasse celui de n'importe
  quel chunk pris seul.
- **Le TOAST** est l'endroit où vivent les lots compressés : 538 Mo sur les
  608 Mo.

## Par état

| | Non compressé | Compressé |
| :--- | ---: | ---: |
| Octets par état, index compris | 260 o | 10,6 o |
| Un jour, à 48 902 états | 12,7 Mo | 0,5 Mo |
| Un an, au même rythme | 4,6 Go | **189 Mo** |

Mesuré sur de vrais chunks de cette base : 84 Mo pour 336 801 états dans le
chunk en cours, 4200 ko pour 404 737 états dans le dernier compressé.

## Chunk par chunk

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

Sur les 112 chunks compressés : **moyenne 28,8×, médiane 25,9×, de 13,3× à
94,7×**. L'écart vient de l'historique lui-même, pas de la compression — une
semaine où une poignée d'entités répètent la même valeur se compresse en
presque rien ; une semaine où une douzaine de capteurs bavards rapportent des
valeurs réellement différentes, non. Les chunks récents tournent autour de 22×,
sous le chiffre global, parce que l'effondrement des index ci-dessus ne se
compte qu'une fois, sur l'hypertable entière.

## Pourquoi ça se compresse aussi bien

L'historique de Home Assistant est le cas idéal pour une compression en
colonnes, et la base le dit. Sur 50 000 états consécutifs :

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

Cinq cents valeurs distinctes pour cinquante mille lignes. Le blob
`attributes`, 120 octets en moyenne contre 8 pour l'état lui-même, se répète
presque autant — la même unité, la même device class, le même nom convivial, sur
chaque ligne. Rangé en colonnes plutôt qu'en lignes, cela devient un
dictionnaire de quelques centaines d'entrées plus un index dedans ; les
horodatages, strictement croissants, deviennent des deltas.

C'est aussi la limite honnête du chiffre : une instance qui enregistre surtout
des capteurs numériques qui changent vraiment, attributs retirés par
`exclude_attributes`, compressera moins. Une instance qui enregistre mille
capteurs binaires compressera davantage.

## Compressé ne veut pas dire plus lent à lire

C'est l'inverse, pour les requêtes que fait un tableau de bord. La même moyenne
horaire, sur une semaine compressée et sur la semaine en cours d'écriture :

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

| Semaine | Chunk | Lignes lues | Temps |
| :--- | :--- | ---: | ---: |
| 01-10 → 08-10 | compressé | 12 991 | **0,97 ms** |
| 15-10 → 22-10 | non compressé | 8 084 | 6,93 ms |

Sept fois plus rapide sur 1,6× plus de lignes — environ 11× par ligne. Un chunk
compressé lit une poignée de lots au lieu de parcourir 13 000 versions de
lignes, et les colonnes que la requête ne mentionne jamais ne sont pas lues du
tout.

Ce qu'un chunk compressé fait plus lentement, c'est se faire écrire dedans.
C'est tout l'objet de `compress_after` : tenir la compression à l'écart du chunk
qui reçoit encore des états. Les lignes qui arrivent en retard — un backfill, un
script de migration, un rejeu après une base injoignable — atterrissent dans un
chunk déjà compressé, sont écrites dans sa partie non compressée, puis
recompressées plus tard.

## Le lire depuis Home Assistant

Avec `enable_stats_size: true`, ces trois mêmes valeurs sont des capteurs —
*States Original Size*, *States Compressed Size* et *States Compression Ratio* —
rafraîchis toutes les `stats_size_interval` minutes, avec les comptes de chunks
à côté sous `enable_stats_chunk`. Sans SQL, et sans seconde connexion à la base.

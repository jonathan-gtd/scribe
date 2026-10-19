# La compresión, medida

[← README](../README.es.md) · leído el 2026-10-19

No es un banco de pruebas. Una instancia de Home Assistant, la base de datos
detrás de este README, con los ajustes por defecto de Scribe: chunks de 7 días,
comprimidos en cuanto cumplen siete días, nada afinado.

| | |
| :--- | :--- |
| Historial | 53 147 521 estados de 1 948 entidades, desde el 2024-08-19 |
| Escrito al día | 48 902 estados, 0,57 por segundo |
| Chunks | 114, de los cuales 112 comprimidos |
| Antes de comprimir | 18 GB |
| Después de comprimir | 608 MB |
| **Ratio** | **29,7×** |
| Hypertable en disco | **733 MB**, incluidos los dos chunks sin comprimir |

TimescaleDB lo mide por sí mismo: la cifra se reproduce en cualquier base de
datos de Scribe en vez de citarse.

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

## Adónde va el espacio

El total esconde tres historias distintas:

| | Antes | Después |
| :--- | ---: | ---: |
| Tabla | 11 GB | 64 MB |
| Índices | 6867 MB | 6760 kB |
| TOAST | 960 kB | 538 MB |
| **Total** | **18 GB** | **608 MB** |

- **La tabla** guarda los estados en sí: 11 GB pasan a 64 MB de esqueleto de
  chunk — los datos se van al TOAST, comprimidos.
- **Los índices** casi dejan de existir: 6867 MB frente a 6760 kB, unas 1000×.
  Un chunk comprimido no necesita un B-tree sobre `(metadata_id, time)` —
  conserva un mínimo y un máximo por lote y lee el lote. Ese desplome supone un
  tercio de los 18 GB, y por eso el ratio global supera al de cualquier chunk
  por separado.
- **El TOAST** es donde viven los lotes comprimidos: 538 MB de los 608 MB.

## Por estado

| | Sin comprimir | Comprimido |
| :--- | ---: | ---: |
| Bytes por estado, índices incluidos | 260 B | 10,6 B |
| Un día, a 48 902 estados | 12,7 MB | 0,5 MB |
| Un año, al mismo ritmo | 4,6 GB | **189 MB** |

Medido sobre chunks reales de esta base: 84 MB para 336 801 estados en el chunk
en curso, 4200 kB para 404 737 estados en el último comprimido.

## Chunk a chunk

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

Sobre los 112 chunks comprimidos: **media 28,8×, mediana 25,9×, de 13,3× a
94,7×**. La dispersión viene del historial mismo, no de la compresión — una
semana en la que un puñado de entidades repiten el mismo valor se comprime hasta
casi nada; una semana con una docena de sensores habladores que informan valores
realmente distintos, no. Los chunks recientes rondan el 22×, por debajo de la
cifra global, porque el desplome de los índices se cuenta una sola vez, sobre la
hypertable entera.

## Por qué se comprime tan bien

El historial de Home Assistant es el caso ideal para una compresión en columnas,
y la base lo dice. Sobre 50 000 estados consecutivos:

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

Quinientos valores distintos para cincuenta mil filas. El blob `attributes`, 120
bytes de media frente a 8 del estado en sí, se repite casi igual — la misma
unidad, la misma device class, el mismo nombre descriptivo, en cada fila.
Guardado en columnas y no en filas, eso es un diccionario de unos cientos de
entradas más un índice sobre él; las marcas de tiempo, estrictamente
crecientes, se vuelven deltas.

Esa es también la limitación honesta de la cifra: una instancia que registra
sobre todo sensores numéricos que cambian de verdad, con los atributos
eliminados por `exclude_attributes`, comprimirá menos. Una que registra mil
sensores binarios comprimirá más.

## Comprimido no significa más lento de leer

Al contrario, para las consultas que hace un panel. La misma media horaria,
sobre una semana comprimida y sobre la que se está escribiendo:

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

| Semana | Chunk | Filas leídas | Tiempo |
| :--- | :--- | ---: | ---: |
| 01-10 → 08-10 | comprimido | 12 991 | **0,97 ms** |
| 15-10 → 22-10 | sin comprimir | 8 084 | 6,93 ms |

Siete veces más rápido sobre 1,6× más filas — unas 11× por fila. Un chunk
comprimido lee un puñado de lotes en lugar de recorrer 13 000 versiones de fila,
y las columnas que la consulta nunca menciona no se leen en absoluto.

Lo que un chunk comprimido hace más despacio es dejarse escribir. De eso trata
`compress_after`: mantener la compresión lejos del chunk que todavía recibe
estados. Las filas que llegan tarde — un backfill, un script de migración, una
reproducción tras una base inalcanzable — caen en un chunk ya comprimido, se
escriben en su parte sin comprimir y se recomprimen más tarde.

## Leerlo desde Home Assistant

Con `enable_stats_size: true`, esas mismas tres cifras son sensores — *States
Original Size*, *States Compressed Size* y *States Compression Ratio* —
actualizados cada `stats_size_interval` minutos, con los recuentos de chunks al
lado bajo `enable_stats_chunk`. Sin SQL y sin una segunda conexión a la base.

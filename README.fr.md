<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="brands_assets/dark_logo.png">
  <img src="brands_assets/logo.png" alt="Scribe" width="300">
</picture>

### L'historique de Home Assistant dans TimescaleDB

Chaque état et chaque événement, via `asyncpg` — sans bloquer la boucle d'événements.

[![Release](https://img.shields.io/github/v/release/jonathan-gtd/scribe?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases/latest) [![Downloads](https://img.shields.io/github/downloads/jonathan-gtd/scribe/total?color=41BDF5)](https://github.com/jonathan-gtd/scribe/releases) [![Tests](https://img.shields.io/github/actions/workflow/status/jonathan-gtd/scribe/tests.yaml?branch=master&label=tests)](https://github.com/jonathan-gtd/scribe/actions/workflows/tests.yaml) [![License](https://img.shields.io/github/license/jonathan-gtd/scribe?color=lightgrey)](LICENSE)

[![lang en](https://img.shields.io/badge/lang-en-lightgrey)](README.md) [![lang fr](https://img.shields.io/badge/lang-fr-41BDF5)](README.fr.md) [![lang es](https://img.shields.io/badge/lang-es-lightgrey)](README.es.md) [![lang de](https://img.shields.io/badge/lang-de-lightgrey)](README.de.md)

</div>

---

Le recorder de Home Assistant garde quelques semaines d'historique dans SQLite et ralentit à mesure qu'il grossit. Scribe écrit les mêmes états et événements dans **TimescaleDB**, où des années restent rapides et occupent une fraction de la place.

- 🚀 **Asynchrone de bout en bout** — `asyncpg` et des `COPY` par lots : l'enregistrement ne bloque jamais Home Assistant.
- 🗜️ **Compressé automatiquement** — l'historique ancien est découpé en chunks et compressé, typiquement 10× plus petit.
- 🛟 **Rien n'est perdu** — une base indisponible est mise en tampon, puis écrite à son retour.
- 🧩 **Le contexte avec** — entités, appareils, pièces, utilisateurs et intégrations, pas seulement des valeurs.
- 🩺 **Il dit quand quelque chose ne va pas**, dans Repairs plutôt que dans un journal que personne ne lit.

---

## Installation

**1. Une base TimescaleDB.** L'extension est obligatoire — voir *Installer TimescaleDB* plus bas.

**2. Scribe, via HACS :**

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=jonathan-gtd&repository=scribe&category=integration)

*Ou à la main :* copiez `custom_components/scribe` dans votre dossier `custom_components`. Dans les deux cas, redémarrez Home Assistant.

**3. L'URL de la base.** Allez dans **Paramètres → Appareils et services → Ajouter une intégration**, cherchez **Scribe**, et collez-la :

[![Open your Home Assistant instance and start setting up a new integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=scribe)

```
postgresql://scribe:password@192.168.1.10:5432/scribe
```

*Ou dans `configuration.yaml`*, si vous préférez garder votre configuration dans des fichiers :

```yaml
scribe:
  db_url: "postgresql://scribe:password@192.168.1.10:5432/scribe"
```

Voilà — les états sont enregistrés, découpés et compressés, avec le contexte entité, appareil et pièce. Tout ce qui suit est facultatif.

---

<details>
<summary><b>🧩 Scribe Card — des graphiques sur votre tableau de bord</b></summary>
<br>

**[Scribe Card](https://github.com/jonathan-gtd/scribe-card)** affiche n'importe quelle requête de votre historique sur un tableau de bord. Dessinée avec Apache ECharts — la bibliothèque qu'utilisent les graphiques d'historique de Home Assistant — et configurée dans un formulaire, où le type de graphique, l'unité et les axes se choisissent parmi les colonnes que votre requête renvoie.

Elle passe par le service `scribe.query`, donc **aucune seconde connexion à la base à configurer, et aucun mot de passe dans votre tableau de bord**. Installez-la via HACS en dépôt personnalisé, catégorie *Tableau de bord*.

</details>

<details>
<summary><b>🗄️ Installer TimescaleDB</b></summary>
<br>

### Mise en place de la base de données

Il vous faut une instance TimescaleDB en fonctionnement. Je recommande PostgreSQL 17 ou 18.

> **❗ Important** — **L'extension TimescaleDB est obligatoire.** Le découpage en chunks, la
> compression, la rétention et les capteurs de taille sont toute la raison
> d'être de Scribe, et aucun n'existe sur PostgreSQL nu. Une nouvelle
> installation est refusée si l'extension manque — Scribe l'active toutefois
> lui-même lorsque le serveur en dispose et que votre utilisateur PostgreSQL a
> le droit `CREATE` sur la base, ce que la procédure ci-dessous accorde. Les
> installations qui tournent déjà sans elle continuent d'enregistrer et sont
> informées de ce qui leur manque par un problème dans Repairs.

#### Option A : Home Assistant OS (module complémentaire)

Sous Home Assistant OS, je recommande le [module complémentaire TimescaleDB](https://github.com/expaso/hassos-addon-timescaledb).

[![Ouvrir votre instance Home Assistant et afficher la boîte de dialogue d'ajout de dépôt de modules complémentaires avec une URL pré-remplie.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Fexpaso%2Fhassos-addon-timescaledb)

#### Option B : Docker (manuel)

```bash
# Haute disponibilité (recommandé)
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg18

# Standard
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb:pg18
```

Créez la base et l'utilisateur :

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
<summary><b>⚙️ Toutes les options, avec leurs valeurs par défaut</b></summary>
<br>

### Configuration complète (valeurs par défaut)

```yaml
scribe:
  # La seule option obligatoire.
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe

  # Tout le reste est facultatif. Voici les valeurs par défaut.

  # Où il enregistre
  db_schema: ""                 # vide = le schéma de la connexion, normalement public
  db_ssl: false                 # TLS vers la base
  ssl_root_cert: ""             # certificat CA ; lu uniquement si db_ssl est true
  ssl_cert_file: ""             # certificat client, pour le TLS mutuel
  ssl_key_file: ""              # sa clé privée

  # Ce qu'il enregistre
  record_states: true           # les changements d'état
  record_events: false          # les événements Home Assistant (automatisations, scripts…)
  include_domains: []           # vide = tous les domaines
  include_entities: []          # vide = toutes les entités
  include_entity_globs: []      # ex. sensor.meteo_*
  exclude_domains: []           # appliqué après les listes d'inclusion
  exclude_entities: []
  exclude_entity_globs: []
  exclude_attributes: []        # attributs retirés de la colonne attributes
  include_events: []            # vide = tous les types d'événements
  exclude_events: []            # appliqué après include_events

  # Combien de temps il le garde
  chunk_time_interval: "7 days" # durée couverte par un chunk
  compress_after: "7 days"      # les chunks plus vieux que ça sont compressés
  retention_states: ""          # vide = pour toujours ; sinon SUPPRIME les états plus anciens
  retention_events: ""          # vide = pour toujours ; sinon SUPPRIME les événements plus anciens
  enable_rollups: false         # résumés horaires et journaliers des états numériques

  # Comment il écrit
  batch_size: 500               # lignes mises en tampon avant une écriture
  flush_interval: 5             # secondes avant d'écrire un lot incomplet
  max_queue_size: 10000         # lignes gardées en mémoire avant d'écarter les nouvelles
  buffer_on_failure: true       # continuer à tamponner tant que la base est injoignable

  # Ce que scribe.query a le droit de coûter
  query_timeout: 60             # secondes qu'une requête peut durer
  query_max_rows: 20000         # lignes qu'elle peut renvoyer avant d'être refusée

  # Capteurs sur Scribe lui-même
  enable_stats_io: false        # compteurs du writer, lus en mémoire
  enable_stats_chunk: false     # nombre de chunks, une requête par rafraîchissement
  enable_stats_size: false      # tailles sur disque, une requête par rafraîchissement
  stats_io_interval: 60         # secondes entre deux valeurs d'écriture
  stats_chunk_interval: 60      # minutes entre deux requêtes de chunks
  stats_size_interval: 60       # minutes entre deux requêtes de taille

  # Tables de contexte, synchronisées avec les registres de Home Assistant
  enable_table_areas: true
  enable_table_devices: true
  enable_table_integrations: true
  enable_table_users: true
```

</details>

<details>
<summary><b>📋 Référence des paramètres</b></summary>
<br>

| Paramètre | Description |
| :--- | :--- |
| `db_url` | **Obligatoire.** Chaîne de connexion vers votre base TimescaleDB. |
| `db_ssl` | Activer SSL/TLS pour la connexion à la base. |
| `ssl_root_cert` | Chemin vers le fichier CA (ex. `/ssl/ca.crt`). Un chemin relatif est résolu depuis le répertoire de configuration de Home Assistant. |
| `ssl_cert_file` | Chemin vers le certificat client, pour le TLS mutuel. |
| `ssl_key_file` | Chemin vers la clé privée client, pour le TLS mutuel. |
| `db_schema` | Schéma PostgreSQL dans lequel enregistrer. Vide (défaut) : celui de la connexion, normalement `public`. |
| `chunk_time_interval` | Durée couverte par chaque chunk de la table. Voir *Réglage du stockage* plus bas. |
| `compress_after` | Les chunks plus anciens que cet intervalle sont compressés. Voir *Réglage du stockage* plus bas. |
| `retention_states` | **Supprime** l'historique des états plus ancien que cet intervalle (ex. `"365 days"`). Vide (défaut) : tout est conservé. Voir *Rétention* plus bas. |
| `retention_events` | **Supprime** l'historique des événements plus ancien que cet intervalle. Vide (défaut) : tout est conservé. Voir *Rétention* plus bas. |
| `record_states` | Enregistrer ou non les changements d'état. |
| `record_events` | Enregistrer ou non les événements. |
| `batch_size` | Nombre d'éléments mis en tampon avant écriture en base. |
| `flush_interval` | Délai maximal (en secondes) avant de vider le tampon. |
| `max_queue_size` | Nombre maximal d'éléments gardés en mémoire avant d'écarter les nouveaux. |
| `query_timeout` | Secondes pendant lesquelles un appel à `scribe.query` peut tourner avant que la base ne l'arrête (défaut `60`). |
| `query_max_rows` | Lignes qu'un appel à `scribe.query` peut renvoyer avant d'être refusé (défaut `20000`). |
| `buffer_on_failure` | Si vrai, conserve les données en mémoire quand la base est injoignable (jusqu'à `max_queue_size`). |
| `enable_stats_io` | Activer les capteurs de performance de l'écrivain en temps réel (aucune requête en base). |
| `enable_stats_chunk` | Activer les capteurs de nombre de chunks (interrogent la base). |
| `enable_stats_size` | Activer les capteurs de taille de stockage (interrogent la base). |
| `stats_io_interval` | Secondes entre deux valeurs des capteurs d'E/S (défaut `60`). Chaque changement est une ligne que Scribe enregistre sur lui-même. |
| `stats_chunk_interval` | Intervalle (en minutes) de mise à jour des statistiques de chunks. |
| `stats_size_interval` | Intervalle (en minutes) de mise à jour des statistiques de taille. |
| `include_domains` | Liste des domaines à inclure. |
| `include_entities` | Liste des entités précises à inclure. |
| `include_entity_globs` | Liste de motifs d'entités à inclure (ex. `sensor.weather_*`). |
| `exclude_domains` | Liste des domaines à exclure. |
| `exclude_entities` | Liste des entités précises à exclure. |
| `exclude_entity_globs` | Liste de motifs d'entités à exclure (ex. `switch.kitchen_*`). |
| `exclude_attributes` | Liste d'attributs à exclure de la colonne `attributes`. |
| `include_events` | Liste des types d'événements à enregistrer. Laisser vide pour tous les enregistrer. |
| `exclude_events` | Liste des types d'événements à ne jamais enregistrer (appliquée après `include_events`). |
| `enable_table_areas` | Activer la création et la synchronisation de la table `areas`. |
| `enable_table_devices` | Activer la création et la synchronisation de la table `devices`. |
| `enable_table_integrations` | Activer la création et la synchronisation de la table `integrations`. |
| `enable_table_users` | Activer la création et la synchronisation de la table `users`. |
| `enable_rollups` | Conserver des résumés horaires et journaliers pré-calculés des états (`states_hourly`, `states_daily`). Désactivé par défaut. |

</details>

<details>
<summary><b>🗜️ Réglage du stockage — chunks et compression</b></summary>
<br>

Scribe range l'historique dans des **hypertables** TimescaleDB : une table qui
s'utilise et s'interroge comme n'importe quelle autre, mais qui est
physiquement découpée en **chunks**, chacun couvrant une tranche de temps.
Presque tout ce qui touche à l'espace disque et à la vitesse des requêtes
découle de ce découpage — une requête sur la semaine dernière ne lit que les
chunks qui la recouvrent, la compression travaille chunk par chunk, et la
*rétention* plus bas supprime des chunks entiers plutôt que des lignes.

Deux réglages le pilotent, en YAML comme dans l'interface sous
**Configurer → Avancé (TimescaleDB & SSL)** :

### `chunk_time_interval` (défaut `7 days`)

La durée couverte par un chunk.

- **Des chunks plus petits** (ex. `1 day`) : plus de fichiers, plus petits — une
  rétention plus fine, et les requêtes sur des fenêtres récentes touchent moins
  de données. Passé un certain point, une requête sur plusieurs mois doit ouvrir
  des centaines de chunks.
- **Des chunks plus gros** (ex. `30 days`) : moins de fichiers, plus gros —
  mieux pour les requêtes historiques longues, moins bien pour la mémoire.
  La recommandation de TimescaleDB est que les chunks dans lesquels vous écrivez
  tiennent confortablement en mémoire avec leurs index : un chunk surdimensionné
  sur une petite machine pénalise les écritures.

La valeur par défaut convient à une instance Home Assistant classique.
Envisagez `1 day` si vous enregistrez des milliers d'entités, et seulement dans
ce cas.

> **Le changement ne concerne que les nouveaux chunks.** Ceux déjà écrits
> conservent la durée avec laquelle ils ont été créés, rien n'est réécrit ni
> déplacé — vous aurez simplement un mélange d'anciennes et de nouvelles durées,
> ce que TimescaleDB gère nativement.

### `compress_after` (défaut `7 days`)

L'âge à partir duquel TimescaleDB compresse un chunk. La compression réduit
fortement la taille pour ce type de données (beaucoup d'`entity_id` répétés et
des valeurs qui changent lentement), d'où son activation par défaut.

Les chunks compressés restent parfaitement interrogeables — la vue `states` n'y
voit aucune différence. En revanche, y **écrire** est plus lent : c'est pourquoi
la compression n'intervient qu'une fois le chunk assez ancien pour être
considéré comme terminé. Gardez `compress_after` confortablement au-dessus de
l'âge des données que vous écrivez encore ; des états arrivant dans le désordre
(un rattrapage, un script de migration) atterrissent dans d'anciens chunks.

> **Le changement prend effet au redémarrage suivant**, et les chunks déjà
> compressés le restent — le réglage ne décide que du moment où les *prochains*
> le seront.

### Comment les trois réglages s'articulent

| Réglage | Ce qu'il fait | Réversible |
| :--- | :--- | :--- |
| `chunk_time_interval` | La durée couverte par un chunk | Oui — futurs chunks seulement |
| `compress_after` | Quand un chunk est compressé | Oui |
| `retention_states` / `retention_events` | Quand un chunk est **supprimé** | **Non** |

Ils s'appliquent dans cet ordre au même chunk tout au long de sa vie : écrit →
compressé → supprimé. Deux conséquences à connaître :

- Si `compress_after` dépasse votre rétention, les chunks sont supprimés avant
  d'avoir jamais été compressés, et la compression ne sert à rien.
- La rétention supprime des chunks entiers : votre fenêtre réelle est donc
  l'intervalle demandé **plus** jusqu'à un `chunk_time_interval`. Des chunks
  plus petits la resserrent.

Si les capteurs de taille et de chunks sont activés (`enable_stats_size`,
`enable_stats_chunk`), ils rapportent exactement ce que ces réglages produisent :
nombre de chunks, tailles compressée et non compressée, taux de compression.

</details>

<details>
<summary><b>🧹 Rétention — supprimer l'historique ancien automatiquement</b></summary>
<br>

Par défaut, Scribe conserve tout, indéfiniment. Si vous ne voulez garder qu'une
fenêtre bornée — parce que vous agrégez l'historique brut ailleurs, ou
simplement pour plafonner l'espace disque — indiquez un intervalle de rétention
et TimescaleDB supprimera les chunks plus anciens :

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  retention_states: "365 days"
  retention_events: "30 days"
```

Les deux sont aussi disponibles dans l'interface, sous
**Configurer → Avancé (TimescaleDB & SSL)**.

> **⚠️ Attention** — La rétention **supprime les données définitivement**. Il n'y a ni annulation
> ni corbeille : dès qu'un chunk sort de la fenêtre, il est supprimé, et seule
> une sauvegarde peut le ramener. États et événements se configurent séparément,
> ce qui permet de faire expirer des événements bavards tout en conservant
> l'historique des états.

Ce qu'il faut savoir :

- **Aucun réglage signifie toujours « conserver indéfiniment ».** Vider le champ
  dans l'interface et supprimer la ligne de `configuration.yaml` retirent tous
  deux la politique — une valeur importée un jour depuis le YAML n'a jamais le
  droit de survivre à la ligne qui l'a définie.
- **Scribe est propriétaire de la politique de rétention sur ses propres
  tables.** Vider le champ la supprime — y compris une politique que vous auriez
  créée à la main avec `add_retention_policy()`, ce qui est la seule façon pour
  l'interface d'arrêter réellement les suppressions.
- **Cela démarre immédiatement.** TimescaleDB exécute la politique quelques
  secondes après sa création, et non au prochain intervalle quotidien : tout ce
  qui est hors de la fenêtre disparaît dès la première exécution, juste après le
  redémarrage qui l'a activée.
- **La suppression se fait par chunk, pas par ligne.** Un chunk n'est supprimé
  que lorsque *toutes* ses lignes sont plus anciennes que l'intervalle : avec le
  `chunk_time_interval` par défaut de 7 jours, vous conservez donc jusqu'à une
  semaine de plus que demandé. C'est ce qui rend la rétention quasi gratuite :
  elle supprime des fichiers plutôt que des lignes.
- **Seul l'historique est supprimé.** La table `entities` et les autres tables
  de métadonnées ne sont pas touchées : une entité dont l'historique a
  entièrement expiré reste résolue.
- **TimescaleDB est indispensable** — c'est l'extension qui exécute la
  politique. Sur PostgreSQL nu, indiquer un intervalle de rétention déclenche un
  problème dans Repairs au lieu de ne rien faire en silence.
- Les valeurs acceptées sont des intervalles simples : `30 days`, `6 months`,
  `1 year`. Toute autre valeur est refusée avec une erreur plutôt qu'envoyée à
  la base.

</details>

<details>
<summary><b>📈 Résumés — agrégats horaires et journaliers</b></summary>
<br>

Un an d'un capteur qui remonte une valeur toutes les 30 secondes, c'est environ un million de lignes. Un graphique sur cette année les lit toutes, à chaque affichage.

Avec `enable_rollups: true`, TimescaleDB tient à jour deux résumés de vos états au fil de l'écriture — horaire et journalier — et un graphique sur plusieurs années lit des milliers de lignes au lieu de millions.

```yaml
scribe:
  enable_rollups: true
```

C'est aussi dans l'interface, sous **Configurer → Tables de métadonnées**. Cela ajoute deux vues :

| Vue | Une ligne par | Colonnes |
| --- | --- | --- |
| `states_hourly` | entité et heure | `entity_id`, `bucket`, `value_avg`, `value_min`, `value_max`, `samples` |
| `states_daily` | entité et jour | les mêmes |

```sql
SELECT bucket, value_avg, value_min, value_max
FROM states_daily
WHERE entity_id = 'sensor.temperature_exterieure'
  AND bucket > now() - interval '2 years'
ORDER BY bucket;
```

Seuls les états numériques sont résumés — la moyenne de `on` et `off` n'a aucun sens — donc `value_avg`, `value_min` et `value_max` sont vides pour les autres, tandis que `samples` compte tous les états du bucket.

**Ce sont des données dérivées.** Rien de ce qui compte n'est dupliqué : désactiver l'option supprime les deux vues, la réactiver les reconstruit depuis l'historique, et vos états ne sont jamais touchés. TimescaleDB les rafraîchit lui-même — l'horaire toutes les 30 minutes, le journalier toutes les heures — et chaque passage regarde assez loin en arrière (3 jours, 30 jours) pour qu'un lot écrit en retard y arrive quand même. Scribe se contente de les créer.

</details>

<details>
<summary><b>🗃️ Enregistrer dans un schéma PostgreSQL précis</b></summary>
<br>

Par défaut, Scribe enregistre dans le schéma vers lequel pointe déjà votre connexion — normalement `public`. Renseignez `db_schema` et il crée ce schéma et y met tout : ses tables, ses vues, ses hypertables et ses politiques.

```yaml
scribe:
  db_url: postgresql://scribe:password@192.168.1.10:5432/scribe
  db_schema: scribe
```

C'est aussi dans l'interface, sous **Configurer → Avancé (TimescaleDB & SSL)**.

C'est ce qu'il vous faut quand Scribe partage une base avec autre chose : les tables d'une autre intégration, vos propres copies de l'historique, ou un second Home Assistant qui enregistre sur le même serveur. Les schémas sont indépendants — tables, hypertables, rétention et compression séparées — et rien de ce que Scribe fait dans l'un n'atteint l'autre.

- **Seules les nouvelles données y vont.** Renseigner `db_schema` ne déplace pas l'historique déjà enregistré. Déplacez-le vous-même avant de redémarrer (`ALTER TABLE public.states_raw SET SCHEMA scribe;`), ou interrogez l'ancien schéma directement.
- **Scribe crée le schéma s'il le peut**, ce qui demande `CREATE` sur la base. Un schéma que vous avez créé à la main convient aussi, avec `USAGE` et `CREATE` dessus.
- **Un schéma inaccessible arrête l'enregistrement.** PostgreSQL passe à l'entrée suivante du search path au lieu d'échouer : une faute de frappe remplirait donc `public` pendant que l'interface affiche autre chose. Scribe vérifie où il a atterri et n'enregistre rien plutôt que d'enregistrer au mauvais endroit, avec un problème dans Réparations qui dit quoi accorder.
- **Vos requêtes ne changent pas.** Scribe place le schéma en tête du `search_path` de la connexion, donc `SELECT * FROM states` continue de marcher via `scribe.query`. Depuis Grafana ou psql, qualifiez le nom (`scribe.states`) ou définissez votre propre `search_path`. `public` reste sur le chemin — c'est là que vivent les fonctions TimescaleDB.
- Les valeurs acceptées sont de simples identifiants : lettres, chiffres et tirets bas, ne commençant pas par un chiffre. Vide garde le schéma de la connexion, y compris un que vous auriez fixé vous-même avec `?options=-csearch_path%3Dmonschema` dans l'URL.

**Les tables elles-mêmes** — chaque colonne, leurs relations, et des recettes de requêtes pour Grafana et `scribe.query` — sont documentées dans [`docs/data-structure.md`](docs/data-structure.md).

</details>

<details>
<summary><b>🛠️ Services — flush, query, purge</b></summary>
<br>

### `scribe.flush`
Force l'écriture immédiate en base des données en tampon.

```yaml
service: scribe.flush
```

### `scribe.query`
Exécute une requête SQL en lecture seule sur la base TimescaleDB.

**Paramètres :**
- `sql` (obligatoire) : la requête SQL à exécuter. Ce doit être un `SELECT`.

**Retour :**
Une liste de lignes, chaque ligne étant un dictionnaire nom de colonne → valeur.

**Exemple :**
```yaml
service: scribe.query
data:
  sql: "SELECT * FROM states ORDER BY time DESC LIMIT 5"
response_variable: query_result
```

### `scribe.purge`
Supprime l'historique enregistré. **C'est irréversible.**

**Paramètres** (au moins un des deux premiers est requis) :
- `entity_id` : les entités à purger. Sans `keep_days`, tout leur historique *et* leur ligne dans la table `entities` sont supprimés — les réenregistrer repart de zéro.
- `keep_days` : supprime tout ce qui est plus ancien que ce nombre de jours.
- `events` (par défaut `false`) : supprime aussi les événements plus anciens que `keep_days`. Sans cette durée, l'option est ignorée.

**Retourne :** le nombre d'états, d'événements et de lignes d'entités supprimés.

**Exemples :**
```yaml
# Retirer complètement une entité de la base
action: scribe.purge
data:
  entity_id: sensor.capteur_dont_je_ne_veux_plus
```

```yaml
# Élaguer tout ce qui a plus de deux ans, événements compris
action: scribe.purge
data:
  keep_days: 730
  events: true
response_variable: purged
```

L'historique compressé est purgé lui aussi : TimescaleDB s'en charge et les chunks restent compressés. Pour une fenêtre glissante appliquée en continu, utilisez plutôt les réglages de *rétention* plus bas : une purge est ponctuelle.

</details>

<details>
<summary><b>📊 Capteurs de statistiques</b></summary>
<br>

Activez les capteurs en positionnant leurs options dans votre configuration.

### Statistiques d'écriture (`enable_stats_io: true`)

Mesures en temps réel issues de l'écrivain (aucune requête en base).

| Capteur | Description |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_states_written` | Nombre total de changements d'état écrits en base. |
| <img src="https://api.iconify.design/mdi:database-plus.svg?color=%232196F3" width="15" /> `sensor.scribe_events_written` | Nombre total d'événements écrits en base. |
| <img src="https://api.iconify.design/mdi:buffer.svg?color=%232196F3" width="15" /> `sensor.scribe_buffer_size` | Nombre d'éléments actuellement en attente dans le tampon mémoire. |
| <img src="https://api.iconify.design/mdi:timer-sand.svg?color=%232196F3" width="15" /> `sensor.scribe_last_write_duration` | Durée (en ms) de la dernière écriture en base. |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_states_rate` | Débit d'états écrits en base (par minute). |
| <img src="https://api.iconify.design/mdi:speedometer.svg?color=%232196F3" width="15" /> `sensor.scribe_events_rate` | Débit d'événements écrits en base (par minute). |

### Statistiques de chunks (`enable_stats_chunk: true`)

Nombre de chunks (mis à jour toutes les `stats_chunk_interval` minutes).

| Capteur | Description |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_chunks` | Nombre total de chunks de la table des états. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_chunks` | Nombre de chunks déjà compressés. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_chunks` | Nombre de chunks en attente de compression. |
| <img src="https://api.iconify.design/mdi:cube-outline.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_chunks` | Nombre total de chunks de la table des événements. |
| <img src="https://api.iconify.design/mdi:package-down.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_chunks` | Nombre de chunks d'événements compressés. |
| <img src="https://api.iconify.design/mdi:package-up.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_chunks` | Nombre de chunks d'événements non compressés. |

### Statistiques de taille (`enable_stats_size: true`)

Espace occupé, en octets (mis à jour toutes les `stats_size_interval` minutes).

| Capteur | Description |
| :--- | :--- |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_states_total_size` | Taille totale sur disque (données compressées + chunks récents + index). |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_states_original_size` | **Taille théorique** si les données n'étaient pas compressées (ex. 11 Go). |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compressed_size` | Taille physique des chunks de données compressés. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_states_uncompressed_size` | Taille des données récentes pas encore compressées (ou index en attente). |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_states_compression_ratio` | Taux de compression des états (%). |
| <img src="https://api.iconify.design/mdi:database.svg?color=%232196F3" width="15" /> `sensor.scribe_events_total_size` | Taille totale sur disque de la table des événements. |
| <img src="https://api.iconify.design/mdi:database-search.svg?color=%232196F3" width="15" /> `sensor.scribe_events_original_size` | Taille théorique des événements avant compression. |
| <img src="https://api.iconify.design/mdi:package-variant.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compressed_size` | Taille des données d'événements compressées. |
| <img src="https://api.iconify.design/mdi:package-variant-closed.svg?color=%232196F3" width="15" /> `sensor.scribe_events_uncompressed_size` | Taille des données d'événements non compressées. |
| <img src="https://api.iconify.design/mdi:percent.svg?color=%232196F3" width="15" /> `sensor.scribe_events_compression_ratio` | Taux de compression des événements (%). |

</details>

<details>
<summary><b>🖼️ Tableau de bord</b></summary>
<br>

Une mise en page Lovelace prête à l'emploi rassemblant tous les capteurs utiles
de Scribe (statistiques de base, taux de compression, performances d'écriture)
est disponible dans ce dépôt, en deux variantes :

| Fichier | Ce que c'est | Où le coller |
| --- | --- | --- |
| [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml) | Une **carte unique** (`type: vertical-stack`) | L'éditeur YAML de carte (« Ajouter une carte » → « Manuel ») |
| [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml) | Une **vue entière** (`title` / `icon` / `cards`) | L'éditeur YAML de vue |

> ⚠️ Les deux ne sont pas interchangeables. Coller le fichier de *vue* dans un
> éditeur de *carte* échoue avec **« Aucun type de carte configuré »**, car une
> configuration de carte doit commencer par une clé `type:`.

**Option A — l'ajouter comme carte (le plus simple, fonctionne dans tous les types de vue) :**

1.  Ouvrez votre tableau de bord et cliquez sur « Modifier le tableau de bord » (icône crayon).
2.  Cliquez sur **+ Ajouter une carte** et descendez tout en bas du sélecteur pour choisir **Manuel**.
3.  Copiez le contenu de [`lovelace_scribe_card.yaml`](lovelace_scribe_card.yaml), remplacez tout ce qui se trouve dans l'éditeur, puis cliquez sur **Enregistrer**.

**Option B — l'ajouter comme vue dédiée :**

1.  Ouvrez votre tableau de bord et cliquez sur « Modifier le tableau de bord » (icône crayon).
2.  Cliquez sur le bouton **+** *dans la barre d'onglets du haut* (à côté du nom de vos vues) pour ajouter une vue — et non sur le bouton « Ajouter une carte ».
3.  Dans la boîte de dialogue de la vue, ouvrez le menu ⋮ (ou le bouton « Afficher l'éditeur de code ») et choisissez **Modifier en YAML**.
4.  Copiez le contenu de [`lovelace_scribe_view.yaml`](lovelace_scribe_view.yaml), remplacez tout ce qui se trouve dans l'éditeur, puis cliquez sur **Enregistrer**.

</details>

<details>
<summary><b>📦 Migrer depuis InfluxDB, LTSS, le recorder ou Scribe 2.x</b></summary>
<br>

### Mise à jour depuis Scribe 2.x

Scribe 3.0 a remplacé la table `states` par `states_raw` accompagnée d'une vue de
compatibilité, et a donné à `entities` une clé primaire numérique. La conversion
d'une ancienne base était assurée par les versions 3.x et a été **supprimée en
3.9**.

Si votre base contient encore une *table* `states` (et non une vue), une table
`states_legacy`, ou une table `entities` sans colonne `id`, Scribe s'arrête au
démarrage, n'enregistre rien et signale un problème dans Repairs — sans rien
renommer, créer ni supprimer. Installez **Scribe 3.8**, laissez Home Assistant
tourner jusqu'à ce que les logs annoncent la fin de la migration (une quinzaine
de minutes sur une grosse base), puis remettez à jour.

Les installations neuves et toute base créée par une version 3.x ne sont pas
concernées.

### Reprise de données depuis d'autres sources

`migration/` contient trois scripts qui recopient un historique dans Scribe depuis ailleurs. Ils se lancent à la main, une fois, depuis une machine qui atteint les deux bases — et Scribe doit avoir démarré au moins une fois, pour que ses tables existent.

```bash
cd migration
pip install psycopg2-binary python-dotenv   # plus influxdb-client pour InfluxDB
cp .env.example .env && nano .env
python3 <script>.py
```

| Source | Script | À renseigner |
| --- | --- | --- |
| InfluxDB | `influx2scribe.py` | `INFLUX_*` |
| LTSS | `ltss2scribe.py` | `LTSS_*` |
| Recorder de Home Assistant | `recorder2scribe.py` | `RECORDER_*`, avec `RECORDER_TYPE` à `postgres` ou `sqlite` (SQLite ne demande que `RECORDER_DB_PATH`) |

Chaque exécution a aussi besoin de `SCRIBE_*` — la destination — et des réglages de migration : `MIGRATION_START_TIME`, `MIGRATION_END_TIME`, `CHUNK_SIZE` (heures par lot) et `PURGE_DESTINATION`, qui **supprime l'historique de la destination avant d'importer**. Laissez-le à `False` sauf si c'est voulu.

Chaque script vérifie le schéma de destination avant d'écrire quoi que ce soit, et s'arrête avec une explication plutôt qu'un mur d'erreurs ligne par ligne si Scribe ne l'a jamais initialisé.

</details>

<details>
<summary><b>🩺 Dépannage</b></summary>
<br>

### À regarder en premier

Deux endroits répondent à « pourquoi rien n'est enregistré ? » sans lire une seule ligne de log :

- **Paramètres → Appareils et services → Scribe → ⋮ → Télécharger les diagnostics** indique
  ce que fait réellement l'écrivain : connecté ou non, TimescaleDB trouvé ou non, combien
  d'éléments attendent dans le tampon et combien ont été écartés, les échecs d'écriture
  consécutifs, les réglages de stockage et de rétention en vigueur. L'URL de la base n'y
  figure jamais, et les erreurs du pilote sont expurgées de toute chaîne de connexion.
- **Paramètres → Système → Réparations** liste les problèmes ci-dessous, et
  **Paramètres → Système → État du système** indique vers quelle base Scribe pointe et
  s'il y est connecté à l'instant.

### Repairs

Scribe signale les problèmes qu'il ne peut pas résoudre seul dans
**Paramètres → Système → Réparations**, pour vous éviter de surveiller les logs.
Chacun disparaît de lui-même une fois la cause corrigée.

| Problème | Ce que cela signifie |
| --- | --- |
| Base de données injoignable | La connexion a échoué. Scribe continue de mettre en tampon et réessaie en arrière-plan : l'historique enregistré pendant la panne est écrit dès que la base revient. Vérifiez que le serveur tourne et que l'URL et les identifiants sont corrects. |
| Écriture impossible en base | Plusieurs écritures consécutives ont échoué. Les données sont gardées en mémoire et écrites au rétablissement — sauf si Home Assistant redémarre avant. |
| Tampon plein | Les écritures ont échoué assez longtemps pour saturer le tampon ; les enregistrements les plus anciens sont maintenant écartés. Réparez la base, ou augmentez `max_queue_size`. |
| Enregistrements écartés | Une écriture a échoué alors que la mise en tampon est désactivée : les enregistrements ont été perdus immédiatement. Activez la mise en tampon pour survivre aux coupures brèves. |
| Impossible de créer ses tables | Scribe a joint la base mais n'a pas pu construire son schéma, généralement un problème de droits. Sur une base neuve, rien n'est enregistré du tout. |
| Impossible d'atteindre le schéma indiqué | Le schéma de `db_schema` n'existe pas et n'a pas pu être créé, ou l'utilisateur de la base n'y a aucun droit. Rien n'est enregistré — plutôt que de remplir `public` en silence. |
| Impossible de créer la vue `states` | L'historique est enregistré, mais la vue par laquelle passent toutes les requêtes est absente — l'historique paraît vide alors que rien n'est perdu. |
| `states_raw` / `events` n'est pas une hypertable | TimescaleDB est installé mais la table n'a jamais été convertie (cas fréquent quand l'extension est ajoutée *après* le remplissage des tables). Chunks, compression et rétention ne font plus rien. |
| `states_raw` / `events` n'est jamais compressée | La table est bien une hypertable mais n'a aucune politique de compression : elle conserve sa taille non compressée. |
| TLS partiellement appliqué | Scribe se connecte en TLS, mais un certificat configuré n'a pas pu être appliqué — le plus souvent un certificat client : il s'authentifie alors comme un client ordinaire et non comme celui que vous aviez provisionné. |
| TimescaleDB n'est pas installé | L'historique est enregistré, mais le découpage et la compression sont indisponibles : la base grossit bien plus vite et les capteurs de taille restent vides. |
| Base antérieure à la version 3.0 | La base utilise encore le schéma pré-3.0, que cette version ne sait pas convertir. Rien n'est enregistré et rien n'a été modifié — installez Scribe 3.8 pour la convertir, puis remettez à jour. |
| Politique de rétention non appliquée | Vous avez demandé la suppression des données au-delà d'un intervalle et la politique n'a pas pu être créée. Rien n'a été supprimé et rien ne l'est — la table continue de grossir. |
| Renommage d'entité non appliqué | Un renommage est entré en collision avec une ligne existante en base. L'historique de l'entité est réparti sur deux identifiants. |

### Consommation mémoire élevée
- Réduisez `max_queue_size`
- Réduisez `flush_interval` pour écrire plus souvent
- Surveillez `sensor.scribe_buffer_size`

### Réglage des performances

Si la vue `states` est lente (plusieurs secondes par requête), c'est
généralement que le planificateur PostgreSQL choisit un **Hash Join** au lieu
d'un **Nested Loop**, ce qui empêche TimescaleDB d'élaguer efficacement les
chunks.

La cause la plus fréquente est un `random_page_cost` élevé (la valeur par défaut
est `4.0`, optimisée pour les disques durs). Avec du stockage moderne (SSD,
NVMe) ou une base bien mise en cache, abaissez cette valeur :

```sql
-- Voir la valeur actuelle
SHOW random_page_cost;

-- Abaisser la valeur (souvent 1.1)
ALTER SYSTEM SET random_page_cost = 1.1;
SELECT pg_reload_conf();
```

Une valeur plus basse encourage le planificateur à utiliser des jointures par
index (Nested Loops), essentielles aux performances de Scribe sur de gros
volumes.

### Toujours bloqué ?
[Ouvrez un ticket](https://github.com/jonathan-gtd/scribe/issues) sur GitHub avec vos logs et votre configuration. Je serai ravi de vous aider !

</details>

---

## Licence

Licence MIT — voir le fichier LICENSE pour les détails

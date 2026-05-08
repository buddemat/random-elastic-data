# random-elastic-data
Python script that generates random data and stores it in Elasticsearch


## Project structure

```
.
├── .gitignore                     # gitignore
├── city_provider.py               # population-weighted city selection from Destatis data
├── config.yml.template            # example config.yml options file
├── load_all_types_random.py       # main script to generate random documents
├── mapping.json                   # mapping for elasticsearch index
├── random_person.py               # module to generate random (German) persons
├── staedte_komplett.csv           # Destatis municipality data (see Data sources)
└── README.md                      # this file
```


## Usage

### Requirements

There are multiple `requirements.txt` files.

### Configuration

Configuration is layered: hardcoded defaults are overridden by environment variables, which are then overridden by values in `config.yml`.

To use a `config.yml`, copy `config.yml.template` to `config.yml` and fill in your values.

| `config.yml` key              | Environment variable        | Description                                              | Default                  |
|-------------------------------|-----------------------------|----------------------------------------------------------|--------------------------|
| `logging.stdout`              | `ENV_LOGGING_STDOUT`        | Print log messages to stdout                             | `True`                   |
| `logging.filename`            | `ENV_LOGGING_LOGFILENAME`   | Log file path (disabled if unset)                        | `None`                   |
| `logging.lvl`                 | `ENV_LOGGING_LEVEL`         | Log level (`DEBUG`, `INFO`, …)                           | `DEBUG`                  |
| `elastic.es_scheme`           | `ENV_ELASTIC_SCHEME`        | ES URL scheme                                            | `http`                   |
| `elastic.es_host`             | `ENV_ELASTIC_HOST`          | ES hostname                                              | `localhost`               |
| `elastic.es_port`             | `ENV_ELASTIC_PORT`          | ES port                                                  | `9200`                   |
| `elastic.es_user`             | `ENV_ELASTIC_USER`          | ES username (no auth if unset)                           | `None`                   |
| `elastic.es_pass`             | `ENV_ELASTIC_PASS`          | ES password (no auth if unset)                           | `None`                   |
| `elastic.index_name`          | `ENV_ELASTIC_TARGETINDEX`   | Target index name                                        | `all_types_random-2`     |
| `elastic.number_of_shards`    | `ENV_ELASTIC_SHARDS`        | Primary shard count (auto-sized from cluster + doc count if unset) | auto        |
| `elastic.use_ilm`             | `ENV_ELASTIC_USEILM`        | Use ILM mode: lifecycle policy + index template + write alias | `false`           |
| `elastic.ilm_alias`           | `ENV_ELASTIC_ILMALIAS`      | Write alias name (ILM mode only)                             | `all_types_random` |
| `elastic.ilm_rollover_docs`   | `ENV_ELASTIC_ILMROLLOVERDOCS` | Max docs per sub-index before rollover (ILM mode only)     | `50000000`         |
| `generation.n_documents`      | `ENV_GENERATE_NDOCS`        | Number of documents to generate                          | `1000`                   |
| `generation.id_offset`        | `ENV_GENERATE_IDOFFSET`     | Numeric ID offset (for appending to an existing index)   | `0`                      |
| `generation.cities_csv`       | `ENV_GENERATE_CITIESCSV`    | Path to the Destatis municipalities CSV                  | `staedte_komplett.csv`   |
| `generation.seed`             | `ENV_GENERATE_SEED`         | Integer seed for reproducible generation (unset = random) | `None`                  |


## Data sources

`staedte_komplett.csv` is derived from the German municipality directory published by
Statistisches Bundesamt (Destatis). Run `get_data.py` to re-download and regenerate it.

| | |
|---|---|
| **Publisher** | Statistisches Bundesamt (Destatis) |
| **Dataset** | Gemeindeverzeichnis-Informationssystem (GV-ISys) |
| **URL** | https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/ |
| **License** | Datenlizenz Deutschland – Namensnennung – Version 2.0 |
| **License URL** | https://www.govdata.de/dl-de/by-2-0 |

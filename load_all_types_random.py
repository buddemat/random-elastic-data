#!/usr/bin/env python3
'''
Module that generates random documents containing ficticious persons and random
numerical data and uploads them to Elasticsearch using the _bulk API. The goal
is to have demo and testing data covering most data types.

Author: Matthias Budde
Date: 2023
'''

import logging
import math
import sys
import os
import json
import re
import random
from random import randint, getrandbits, uniform
import base64
import yaml
import elasticsearch as es
from elasticsearch import helpers
from faker import Faker
import random_person as rp
import city_provider as cp

faker = Faker()

# Rule-of-thumb constants for auto shard sizing.
# Target ~167 GB/shard (bulk-analytics workload); ~6 KB stored per doc (3 KB raw × 2 for index overhead).
_DOC_SIZE_KB = 6
_TARGET_SHARD_GB = 167

_DATA_ROLES = {'data', 'data_content', 'data_hot', 'data_warm', 'data_cold', 'data_frozen'}


def _get_data_node_count(es_client):
    nodes_info = es_client.nodes.info()
    count = sum(1 for n in nodes_info['nodes'].values()
                if _DATA_ROLES & set(n.get('roles', [])))
    return max(1, count)


def _compute_shards(n_docs, n_nodes=1):
    ''' Auto-size: round up to nearest multiple of n_nodes, targeting _TARGET_SHARD_GB per shard. '''
    total_gb = n_docs * _DOC_SIZE_KB / (1024 * 1024)
    raw = max(n_nodes, math.ceil(total_gb / _TARGET_SHARD_GB))
    return math.ceil(raw / n_nodes) * n_nodes


def _get_max_num_id(es_client, index):
    resp = es_client.search(index=index, size=0,
                            aggs={'max_id': {'max': {'field': 'num_id'}}})
    val = resp['aggregations']['max_id']['value']
    return int(val) if val is not None else 0


def _put_ilm_policy(es_client, policy_name, rollover_docs):
    es_client.ilm.put_lifecycle(
        name=policy_name,
        policy={
            'phases': {
                'hot': {
                    'actions': {
                        'rollover': {
                            'max_docs': int(rollover_docs),
                            'max_primary_shard_size': '50gb',
                        }
                    }
                }
            }
        }
    )


def _put_index_template(es_client, template_name, index_pattern, n_shards, policy_name, alias_name):
    with open('mapping.json', 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    es_client.indices.put_index_template(
        name=template_name,
        index_patterns=[index_pattern],
        template={
            'settings': {
                'number_of_shards': n_shards,
                'number_of_replicas': 0,
                'index.lifecycle.name': policy_name,
                'index.lifecycle.rollover_alias': alias_name,
            },
            'mappings': mapping,
        },
        priority=100
    )


def load_options():
    ''' load options from yaml file or environment vars '''

    config_filename = './config.yml'

    opt_dict = {}
    opt_dict['logging'] = {}
    opt_dict['elastic'] = {}
    opt_dict['generation'] = {}

    env = dict(os.environ)

    opt_dict['logging']['stdout'] = env.get('ENV_LOGGING_STDOUT', True)
    opt_dict['logging']['filename'] = env.get('ENV_LOGGING_LOGFILENAME', None)
    opt_dict['logging']['lvl'] = env.get('ENV_LOGGING_LEVEL', 'DEBUG')

    opt_dict['elastic']['es_scheme'] = env.get('ENV_ELASTIC_SCHEME', 'http')
    opt_dict['elastic']['es_host'] = env.get('ENV_ELASTIC_HOST', 'localhost')
    opt_dict['elastic']['es_port'] = env.get('ENV_ELASTIC_PORT', 9200)
    opt_dict['elastic']['es_user'] = env.get('ENV_ELASTIC_USER', None)
    opt_dict['elastic']['es_pass'] = env.get('ENV_ELASTIC_PASS', None)
    opt_dict['elastic']['index_name'] = env.get('ENV_ELASTIC_TARGETINDEX', 'all_types_random-2')
    opt_dict['elastic']['number_of_shards'] = env.get('ENV_ELASTIC_SHARDS', None)
    opt_dict['elastic']['use_ilm'] = env.get('ENV_ELASTIC_USEILM', False)
    opt_dict['elastic']['ilm_alias'] = env.get('ENV_ELASTIC_ILMALIAS', 'all_types_random')
    opt_dict['elastic']['ilm_rollover_docs'] = env.get('ENV_ELASTIC_ILMROLLOVERDOCS', 50_000_000)
    opt_dict['elastic']['mode'] = env.get('ENV_ELASTIC_MODE', 'abort')

    opt_dict['generation']['n_documents'] = env.get('ENV_GENERATE_NDOCS', 1000)
    opt_dict['generation']['cities_csv'] = env.get('ENV_GENERATE_CITIESCSV', None)
    opt_dict['generation']['seed'] = env.get('ENV_GENERATE_SEED', None)

    try:
        with open(config_filename, 'r', encoding='utf-8') as config_file:
            yml_dict = yaml.safe_load(config_file)

            opt_dict['logging'] = opt_dict.get('logging') | yml_dict.get('logging', {})
            opt_dict['elastic'] = opt_dict.get('elastic') | yml_dict.get('elastic', {})
            opt_dict['generation'] = opt_dict.get('generation') | yml_dict.get('generation', {})
    except EnvironmentError:
        pass

    opt_dict['elastic']['es_url'] = f'{opt_dict.get("elastic").get("es_scheme")}://'\
                                    f'{opt_dict.get("elastic").get("es_host")}:'\
                                    f'{opt_dict.get("elastic").get("es_port")}'

    return opt_dict


def init_logging(lvl='DEBUG', log_to_stdout=True, logfile_name=None):
    ''' initialize logging '''
    log_lvl = logging.getLevelName(lvl)

    logger = logging.getLogger(__name__)

    handlers = []
    if log_to_stdout:
        stdout_handler = logging.StreamHandler(sys.stdout)
        handlers.append(stdout_handler)
    if logfile_name:
        log_file_handler = logging.FileHandler(filename = logfile_name,
                                               encoding = 'utf-8',
                                               mode = 'w')
        handlers.append(log_file_handler)

    logging.basicConfig(
             format = '%(asctime)s %(levelname)-8s %(message)s',
             level = logging.ERROR,
             datefmt = '%Y-%m-%d %H:%M:%S',
             handlers = handlers)

    logger.setLevel(log_lvl)
    return logger


def document_stream(idx_name, amount, cities_csv=None, offset=0):
    ''' generator function for stream of json documents / dicts with random persons '''
    mylogger = logging.getLogger(__name__)

    provider = cp.CityProvider(cities_csv) if cities_csv else cp.get_default_provider()

    for num in range(offset+1, offset+amount+1):
        person = rp.RandomPerson(city_provider=provider)
        if (num - offset) % 1000 == 0:
            mylogger.debug(f'{num - offset} documents generated...')
        yield {"_index": idx_name,
               "_source": { 'uuid': person.uuid,
                            'num_id': num,
                            'created_at': person.created_at,
                            'firstname': person.firstname,
                            'lastname': person.lastname,
                            'nested_name': { 'first': person.firstname, 'last': person.lastname },
                            'birthplace': person.birthplace,
                            'nickname': person.nickname,
                            'gender': person.gender,
                            'date_of_birth': person.date_of_birth,
                            'email_address': person.email_address,
                            'ip_address': person.ip_address,
                            'lefthanded': person.lefthanded,
                            'address_st': person.address_st,
                            'address_no': person.address_no,
                            'city_location': person.city_location,
                            'neighborhood': person.neighborhood,
                            'postal_code': person.postal_code,
                            'state': person.state,
                            'phone_number': person.phone_number,
                            'marital_status': person.marital_status,
                            'num_children': person.num_children,
                            'person_xml': person.to_xml(),
                            'person_json': person.to_json(),
                            'some_const_keyword': 'random',
                            'some_text_without_multi_field': person.favorite_food,
                            'some_text_with_array_multifield_content': [person.favorite_food,
                                                                        person.favorite_color,
                                                                        person.occupation],
                            'some_text_with_ignored_keyword': faker.paragraph(nb_sentences = 20),
                            'some_epoch_date': randint(1000000000000,9999999999999),
                            'some_bool': bool(getrandbits(1)),
                            'some_binary': str(base64.b64encode(person.city.encode('utf-8')))[2:-1],
                            'some_long': randint(-2^63,2^63-1),
                            'some_int': randint(-2^63,2^63-1),
                            'some_short': randint(-32768, 32767),
                            'some_byte': randint(-128, 127),
                            'some_unsigned_long': getrandbits(64),
                            'some_double': uniform(2^-1074,(2-2^-52)*2^1023),
                            'some_float': uniform(2^-149, (2-2^-23)*2^127),
                            'some_half_float': uniform(2^-24, 65504),
                            'some_scaled_float': round(uniform(-32768, 32767),2)
                          }
               }


def _init_simple(es_client, idx_name, n_shards, mode):
    ''' create or resume a single index; returns num_id offset '''
    mylogger = logging.getLogger(__name__)

    if mode not in ('replace', 'resume', 'abort'):
        mylogger.error(f'Unknown mode {mode!r}. Valid values: replace, resume, abort.')
        sys.exit(1)

    exists = bool(es_client.indices.exists(index=idx_name))

    if mode == 'abort' and exists:
        doc_count = es_client.count(index=idx_name)['count']
        mylogger.error(
            f'Index {idx_name!r} already exists ({doc_count} docs). '
            f'Use mode=replace or mode=resume.'
        )
        sys.exit(1)

    if mode == 'resume' and exists:
        offset = _get_max_num_id(es_client, idx_name)
        mylogger.info(f'Resuming index {idx_name!r} from num_id offset {offset}.')
        return offset

    if mode == 'replace':
        mylogger.info(f'Deleting existing index {idx_name!r} if present...')
        es_client.options(ignore_status=[400, 404]).indices.delete(index=idx_name)

    mylogger.info('Loading mapping...')
    with open('mapping.json', 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    mylogger.info(f'Creating index {idx_name!r} with {n_shards} shard(s)...')
    es_client.options(ignore_status=[400]).indices.create(
        index=idx_name, mappings=mapping,
        settings={'number_of_shards': n_shards}
    )
    return 0


def _init_ilm(es_client, alias_name, rollover_docs, n_shards, mode):
    ''' create or resume an ILM-managed index set; returns num_id offset '''
    mylogger = logging.getLogger(__name__)

    policy_name    = f'{alias_name}_policy'
    template_name  = f'{alias_name}_template'
    index_pattern  = f'{alias_name}-*'
    bootstrap_name = f'{alias_name}-000001'

    if mode not in ('replace', 'resume', 'abort'):
        mylogger.error(f'Unknown mode {mode!r}. Valid values: replace, resume, abort.')
        sys.exit(1)

    alias_exists  = bool(es_client.indices.exists_alias(name=alias_name))
    indices_exist = bool(es_client.indices.exists(index=index_pattern))

    if mode == 'abort' and (alias_exists or indices_exist):
        found = []
        if alias_exists:
            alias_data = es_client.indices.get_alias(name=alias_name)
            for idx, info in alias_data.items():
                doc_count = es_client.count(index=idx)['count']
                write_flag = ' [write]' if info['aliases'].get(alias_name, {}).get('is_write_index') else ''
                found.append(f'  {idx}{write_flag}: {doc_count} docs')
        else:
            idx_info = es_client.indices.get(index=index_pattern)
            for idx in idx_info:
                doc_count = es_client.count(index=idx)['count']
                found.append(f'  {idx}: {doc_count} docs (orphaned, no alias)')
        mylogger.error(
            f'Found existing data for {alias_name!r}:\n' +
            '\n'.join(found) +
            '\nUse mode=replace or mode=resume.'
        )
        sys.exit(1)

    if mode == 'resume' and alias_exists:
        _put_ilm_policy(es_client, policy_name, rollover_docs)
        _put_index_template(es_client, template_name, index_pattern, n_shards, policy_name, alias_name)
        offset = _get_max_num_id(es_client, alias_name)
        alias_data = es_client.indices.get_alias(name=alias_name)
        write_idx = next(
            (name for name, info in alias_data.items()
             if info['aliases'].get(alias_name, {}).get('is_write_index')),
            None
        )
        if write_idx:
            write_docs = es_client.count(index=write_idx)['count']
            if write_docs > 0:
                mylogger.info(f'Rolling over write index {write_idx!r} ({write_docs} docs)...')
                es_client.indices.rollover(alias=alias_name)
        mylogger.info(f'Resuming alias {alias_name!r} from num_id offset {offset}.')
        return offset

    if mode == 'replace':
        mylogger.info(f'Deleting existing indices matching {index_pattern!r} if present...')
        es_client.options(ignore_status=[400, 404]).indices.delete(index=index_pattern)

    mylogger.info(f'Creating ILM policy {policy_name!r}...')
    _put_ilm_policy(es_client, policy_name, rollover_docs)
    mylogger.info(f'Creating index template {template_name!r}...')
    _put_index_template(es_client, template_name, index_pattern, n_shards, policy_name, alias_name)
    mylogger.info(f'Creating bootstrap index {bootstrap_name!r}...')
    es_client.options(ignore_status=[400]).indices.create(
        index=bootstrap_name,
        aliases={alias_name: {'is_write_index': True}}
    )
    return 0


def main():
    ''' main function '''
    options = load_options()

    mylogger = init_logging(options.get('logging').get('lvl'),
                            options.get('logging').get('stdout'),
                            options.get('logging').get('filename'))

    seed = options.get('generation').get('seed')
    if seed is not None:
        random.seed(int(seed))
        Faker.seed(int(seed))
        mylogger.debug(f'Random seed set to {seed}')

    options_str = re.sub("('es_pass': )('|\")(.*?)('|\")(, )", r"\1*****\5", str(options))
    mylogger.debug(f'Options loaded: {options_str}')

    mylogger.info(f'Connecting to ES cluster {options.get("elastic").get("es_url")}...')
    es_client = es.Elasticsearch([options.get('elastic').get('es_url')],
                                  basic_auth=(f'{options.get("elastic").get("es_user")}',
                                              f'{options.get("elastic").get("es_pass")}')
                                  )

    n_data_nodes = _get_data_node_count(es_client)
    mylogger.debug(f'Data nodes in cluster: {n_data_nodes}')

    n_docs = int(float(options.get('generation').get('n_documents')))
    shards_opt = options.get('elastic').get('number_of_shards')
    n_shards = int(shards_opt) if shards_opt is not None else _compute_shards(n_docs, n_data_nodes)
    mylogger.info(f'Shard count: {n_shards} '
                  f'({"explicit" if shards_opt else f"auto, {n_data_nodes} data node(s)"}) '
                  f'for {n_docs} documents.')

    mode = str(options.get('elastic').get('mode')).lower()
    use_ilm = str(options.get('elastic').get('use_ilm')).lower() in ('true', '1', 'yes')

    if use_ilm:
        alias = options.get('elastic').get('ilm_alias')
        rollover_docs = options.get('elastic').get('ilm_rollover_docs')
        id_offset = _init_ilm(es_client, alias, rollover_docs, n_shards, mode)
        idx_target = alias
    else:
        idx_target = options.get('elastic').get('index_name')
        id_offset = _init_simple(es_client, idx_target, n_shards, mode)

    n_remaining = n_docs - id_offset
    if n_remaining <= 0:
        mylogger.info(f'Target of {n_docs} documents already reached ({id_offset} exist). Nothing to do.')
        return

    mylogger.info(f'Generating and indexing {n_remaining} document(s) '
                  f'(target: {n_docs}, existing: {id_offset})...')
    stream = document_stream(idx_target,
                             n_remaining,
                             options.get('generation').get('cities_csv'),
                             id_offset)

    for status_ok, response in helpers.streaming_bulk(es_client, actions=stream):
        if not status_ok:
            mylogger.debug(response)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
'''
Module that generates random documents containing ficticious persons and random
numerical data and uploads them to Elasticsearch using the _bulk API. The goal
is to have demo and testing data covering most data types.

Author: Matthias Budde
Date: 2023
'''

import logging
import sys
import os
import json
from random import randint, getrandbits, uniform
import base64
import yaml
import elasticsearch as es
from elasticsearch import helpers
from faker import Faker
import random_person as rp

faker = Faker()


def load_options():
    ''' load options from yaml file or environment vars '''

    config_filename = './config.yml'

    # init empty dict
    opt_dict = {}
    opt_dict['logging'] = {}
    opt_dict['elastic'] = {}
    opt_dict['generation'] = {}

    # load ALL environment vars
    env = dict(os.environ)

    # load relevant env vars, default to standard values if env vars not set
    opt_dict['logging']['stdout'] = env.get('ENV_LOGGING_STDOUT', True)
    opt_dict['logging']['filename'] = env.get('ENV_LOGGING_LOGFILENAME', None)
    opt_dict['logging']['lvl'] = env.get('ENV_LOGGING_LEVEL', 'DEBUG')

    opt_dict['elastic']['es_scheme'] = env.get('ENV_ELASTIC_SCHEME', 'http')
    opt_dict['elastic']['es_host'] = env.get('ENV_ELASTIC_HOST', 'localhost')
    opt_dict['elastic']['es_port'] = env.get('ENV_ELASTIC_PORT', 9200)
    opt_dict['elastic']['es_user'] = env.get('ENV_ELASTIC_USER', None)
    opt_dict['elastic']['es_pass'] = env.get('ENV_ELASTIC_PASS', None)
    opt_dict['elastic']['index_name'] = env.get('ENV_ELASTIC_TARGETINDEX', 'all_types_random-2')

    opt_dict['generation']['n_documents'] = env.get('ENV_GENERATE_NDOCS', 1000)
    opt_dict['generation']['id_offset'] = env.get('ENV_GENERATE_IDOFFSET', 0)

    try:
        with open(config_filename, 'r', encoding='utf-8') as config_file:
            yml_dict = yaml.safe_load(config_file)

            # merge values from config.yml into options dict, needs to be done per sublevel
            opt_dict['logging'] = opt_dict.get('logging') | yml_dict.get('logging', {})
            opt_dict['elastic'] = opt_dict.get('elastic') | yml_dict.get('elastic', {})
            opt_dict['generation'] = opt_dict.get('generation') | yml_dict.get('generation', {})
    except EnvironmentError: # parent of IOError, OSError *and* WindowsError where available
        # if no config.yml exists, work with values from above
        pass

    # build full url
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
             level = logging.ERROR, # root logger
             datefmt = '%Y-%m-%d %H:%M:%S',
             handlers = handlers)

    logger.setLevel(log_lvl) # mylogger
    return logger


def document_stream(idx_name, amount, offset=0):
    ''' generator function for stream of json documents / dicts with random persons '''
    mylogger = logging.getLogger(__name__)

    for num in range(offset+1, offset+amount+1):
        p = rp.RandomPerson()
        if num%1000 == 0:
            mylogger.debug(f'{num} documents generated...')
        yield {"_index": idx_name,
               "_source": { 'uuid': p.uuid,
                            'num_id': num,
                            'created_at': p.created_at,
                            'firstname': p.firstname,
                            'lastname': p.lastname,
                            'nested_name': { 'first': p.firstname, 'last': p.lastname },
                            'birthplace': p.birthplace,
                            'nickname': p.nickname,
                            'age': p.age,
                            'gender': p.gender,
                            'date_of_birth': p.date_of_birth,
                            'email_address': p.email_address,
                            'ip_address': p.ip_address,
                            'lefthanded': p.lefthanded,
                            'person_xml': p.to_xml(),
                            'person_json': p.to_json(),
                            'some_const_keyword': 'random',
                            'some_text_without_multi_field': p.favorite_food,
                            'some_text_with_array_multifield_content': [p.favorite_food,
                                                                        p.favorite_color,
                                                                        p.occupation],
                            'some_text_with_ignored_keyword': faker.paragraph(nb_sentences = 20),
                            'some_epoch_date': randint(1000000000000,9999999999999),
                            'some_bool': bool(getrandbits(1)),
                            'some_binary': str(base64.b64encode(p.city.encode('utf-8')))[2:-1],
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

def init_es_index(es_client, idx_name, replace=False):
    ''' create the initial index '''
    if replace:
        # delete (possibly) existing index
        es_client.options(ignore_status=[400,404]).indices.delete(index=idx_name)

    with open('mapping.json', 'r', encoding='utf-8') as mapping_file:
        mapping = json.load(mapping_file)

        # create index with mapping
        response = es_client.options(ignore_status=[400]).indices.create(
            index = idx_name,
            mappings = mapping
        )
        return response


def main():
    ''' main function '''
    # load options
    options = load_options()

    # init logger
    mylogger = init_logging(options.get('logging').get('lvl'),
                            options.get('logging').get('stdout'),
                            options.get('logging').get('filename'))

    mylogger.debug(f'Options loaded: {options}')

    es_client = es.Elasticsearch([options.get('elastic').get('es_url')],
                                  basic_auth=(f'{options.get("elastic").get("es_user")}',
                                              f'{options.get("elastic").get("es_pass")}')
                                  )

    init_es_index(es_client, options.get('elastic').get('index_name'), replace=True)

    stream = document_stream(options.get('elastic').get('index_name'),
                             options.get('generation').get('n_documents'),
                             options.get('generation').get('id_offset'))

    for status_ok, response in helpers.streaming_bulk(es_client, actions=stream):
        if not status_ok:
            # if failure inserting
            mylogger.debug(response)

if __name__ == '__main__':
    main()

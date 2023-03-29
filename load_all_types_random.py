#!/usr/bin/env python3
'''
Module that generates random documents containing ficticious persons and random
numerical data and uploads them to Elasticsearch using the _bulk API. The goal
is to have demo and testing data covering most data types.
'''
import json
from random import randint, getrandbits, uniform
import base64
import elasticsearch as es
from elasticsearch import helpers
from faker import Faker
import random_person as rp

faker = Faker()


def load_options():
    ''' load options from yaml file or environment vars '''
    # init empty dict
    opt_dict = {}

    opt_dict['es_scheme'] = 'http'
    opt_dict['es_host'] = 'localhost'
    opt_dict['es_port'] = 9200
    opt_dict['es_user'] = None
    opt_dict['es_pass'] = None

    opt_dict['index_name'] = 'all_types_random-2'

    # build full url
    opt_dict['es_url'] = f'{opt_dict["es_scheme"]}://{opt_dict["es_host"]}:{opt_dict["es_port"]}'

    return opt_dict

def document_stream(idx_name, amount):
    ''' generator function for stream of json documents / dicts with random persons '''
    for n in range(1, amount+1):
        p = rp.RandomPerson()
        yield {"_index": idx_name,
               "_source": { 'uuid': p.uuid,
                            'num_id': n,
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

    es_client = es.Elasticsearch([options.get('es_url')],
                                  basic_auth=(f'{options.get("es_user")}',
                                              f'{options.get("es_pass")}')
                                  )

    init_es_index(es_client, options.get('index_name'), replace=True)

    stream = document_stream(options.get('index_name'), 10)

    for status_ok, response in helpers.streaming_bulk(es_client, actions=stream):
        if not status_ok:
            # if failure inserting
            print(response)

if __name__ == '__main__':
    main()

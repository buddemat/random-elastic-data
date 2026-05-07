'''
Module that generates random (German) persons using Faker and the Destatis city dataset.

Author: Matthias Budde
Date: 2023
'''

import json
import math
import random
import uuid as uuid_module
from datetime import datetime
from random import randint, choices, uniform
import xml.etree.ElementTree as ET
from faker import Faker
from faker_food import FoodProvider
import city_provider as cp

fake = Faker(['en', 'de'])
fake['en'].add_provider(FoodProvider)

DOMAINS = [
    'web.de', 'gmx.de', 'gmx.net', 't-online.de', 'freenet.de',
    'posteo.de', 'mail.de', 'online.de',
    'gmail.com', 'yahoo.com', 'yahoo.de', 'hotmail.com', 'hotmail.de',
    'outlook.com', 'outlook.de', 'icloud.com', 'me.com',
    'protonmail.com', 'protonmail.ch', 'mailbox.org',
]

INTERESTS = [
    'Fotografie', 'Kochen', 'Reisen', 'Lesen', 'Radfahren', 'Wandern',
    'Gartenarbeit', 'Yoga', 'Schwimmen', 'Musik', 'Malen', 'Zeichnen',
    'Spielen', 'Klettern', 'Laufen', 'Tanzen', 'Backen', 'Nähen',
    'Kino', 'Theater', 'Camping', 'Angeln', 'Ski fahren', 'Surfen',
    'Motorrad fahren',
]

MARITAL_STATUSES = ['single', 'married', 'divorced', 'widowed']
MARITAL_WEIGHTS  = [38, 45, 13, 4]

NUM_CHILDREN_VALUES  = [0, 1, 2, 3, 4, 5]
NUM_CHILDREN_WEIGHTS = [40, 25, 22, 9, 3, 1]


def _fill_element(el, value):
    if isinstance(value, dict):
        for k, v in value.items():
            _fill_element(ET.SubElement(el, k), v)
    elif isinstance(value, list):
        for item in value:
            _fill_element(ET.SubElement(el, 'item'), item)
    elif value is not None:
        el.text = str(value)


def _dict_to_xml(data, root_tag='person'):
    root = ET.Element(root_tag)
    _fill_element(root, data)
    return ET.tostring(root, encoding='unicode')


def _to_email_slug(s):
    return (s.lower()
             .replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
             .replace('ß', 'ss').replace(' ', '').replace('-', ''))


def _make_email(firstname, lastname, nickname, domain):
    first_slug = _to_email_slug(firstname)
    last_slug  = _to_email_slug(lastname)
    rnd4 = randint(1000, 9999)
    pattern = choices(['fl', 'il', 'nick'], weights=[55, 35, 10])[0]
    if pattern == 'fl':
        local = f'{first_slug}.{last_slug}{rnd4}'
    elif pattern == 'il':
        local = f'{first_slug[0]}{last_slug}{rnd4}'
    else:
        local = nickname
    return f'{local}@{domain}'


class RandomPerson:
    '''Represents a randomly generated person with realistic German attributes.'''
    _instance_count = 0

    def __init__(self, city_provider=None):
        RandomPerson._instance_count += 1

        provider = city_provider if city_provider is not None else cp.get_default_provider()

        self.uuid = str(uuid_module.uuid1())
        self.created_at = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self.num_id = RandomPerson._instance_count

        self.firstname = fake['en'].first_name()
        self.lastname  = fake['de'].last_name()
        self.nested_name = {'first': self.firstname, 'last': self.lastname}

        self.nickname = fake.word() + '_' + fake.word()
        if randint(0, 4) == 0:
            self.nickname += str(randint(10, 99))

        dob = fake.date_of_birth(minimum_age=0, maximum_age=110)
        self.date_of_birth = dob.strftime('%d.%m.%Y')

        self.gender = 'diverse' if random.random() < 0.02 else ('male' if random.random() < 0.5 else 'female')

        birthname_candidate = fake['de'].last_name()
        if birthname_candidate != self.lastname and random.random() < 0.30:
            self.birthname = birthname_candidate

        birthplace_city = provider.random_city()
        self.birthplace = birthplace_city['name']

        wohnort = provider.random_city()
        self.city = wohnort['name']
        self.postal_code = wohnort['postal_code']
        self.state = wohnort['state']

        r_km = math.sqrt(wohnort['area_km2'] / math.pi) / 2
        sigma_lat = min(0.075, r_km / 111)
        sigma_lon = min(0.10,  r_km / (111 * math.cos(math.radians(wohnort['lat']))))
        home_lat = wohnort['lat'] + max(-0.15, min(0.15, random.gauss(0, sigma_lat)))
        home_lon = wohnort['lon'] + max(-0.20, min(0.20, random.gauss(0, sigma_lon)))
        self.city_location = {'lat': round(home_lat, 6), 'lon': round(home_lon, 6)}

        side_km = uniform(0.2, 1.5)
        d_lat = side_km / 111
        d_lon = side_km / (111 * math.cos(math.radians(home_lat)))
        self.neighborhood = {
            'type': 'envelope',
            'coordinates': [
                [round(home_lon - d_lon, 6), round(home_lat + d_lat, 6)],
                [round(home_lon + d_lon, 6), round(home_lat - d_lat, 6)],
            ],
        }

        self.occupation = fake.job()
        self.interests = random.sample(INTERESTS, randint(2, 5))
        self.favorite_color = fake.color_name()
        self.favorite_food = fake['en'].dish()
        self.motto = fake.sentence()

        domain = random.choice(DOMAINS)
        last_slug = _to_email_slug(self.lastname)
        self.email_address = _make_email(self.firstname, self.lastname, self.nickname, domain)
        self.homepage = (f'https://www.{last_slug}.de' if random.random() < 0.6
                         else fake.url())

        self.ip_address = fake.ipv4()
        self.lefthanded = random.random() < 0.10
        self.address_st = fake['de'].street_name()
        _no = fake.numerify(choices(['%', '%#', '%##'], weights=[15, 65, 20])[0])
        self.address_no = _no + (choices(['a', 'b', 'c', 'd'], weights=[4, 3, 2, 1])[0]
                                  if randint(1, 100) <= 8 else '')

        self.phone_number = fake['de'].phone_number()
        self.marital_status = choices(MARITAL_STATUSES, weights=MARITAL_WEIGHTS)[0]
        self.num_children = choices(NUM_CHILDREN_VALUES, weights=NUM_CHILDREN_WEIGHTS)[0]

    def to_dict(self):
        return vars(self)

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4, ensure_ascii=False)

    def to_xml(self):
        return _dict_to_xml(self.to_dict())

'''
Module that generates random (German) persons, built on PyZufall.
'''
import json
from random import randint
import uuid
from datetime import datetime
from pyzufall.person import Person
from dicttoxml import dicttoxml
from faker import Faker

faker = Faker()

class RandomPerson:
    """Class for random person"""
    _instance_count = 0

    def __init__(self, uuid=str(uuid.uuid1()), num_id=None):
        RandomPerson._instance_count += 1
        self.created_at = datetime.today().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        self.uuid = uuid
        self.num_id = num_id if num_id else RandomPerson._instance_count

        p = Person()
        self.firstname = p.vorname
        self.lastname = p.nachname
        self.nested_name = { 'first': p.vorname, 'last': p.nachname }
        self.nickname = p.nickname
        # TODO: make age attribute into property?
        self.age = p.alter
        self.date_of_birth = p.geburtsdatum
        # ~2% should be diverse
        self.gender = 'diverse' if randint(0,100) < 2 else ('male' if p.geschlecht else 'female')
        if p.geburtsname != p.nachname:
            self.birthname = p.geburtsname
        self.birthplace = p.geburtsort
        self.city = p.wohnort
        self.occupation = p.beruf
        self.interests = p.interessen
        self.favorite_color = p.lieblingsfarbe
        self.favorite_food = p.lieblingsessen
        self.motto = p.motto
        self.email_address = p.email
        self.ip_address = faker.ipv4()
        self.homepage = p.homepage
        # ~10% should be lefthanded
        self.lefthanded = bool(randint(0,100) < 10)
 
    def to_dict(self):
        return vars(self)

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4, ensure_ascii=False)

    def to_xml(self):
        return dicttoxml(self.to_dict()).decode('utf-8')

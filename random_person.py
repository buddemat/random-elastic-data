import json
from random import randint
import uuid
from datetime import datetime
from pyzufall.person import Person
from dicttoxml import dicttoxml

class RandomPerson:
    """Class for random person"""
    _instance_count = 0

    def __init__(self, uuid=str(uuid.uuid1())):
        RandomPerson._instance_count += 1
        self.created_at = datetime.today() # created_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        self.uuid = uuid
        self.num_id = RandomPerson._instance_count

        p = Person()
        self.firstname = p.vorname
        self.lastname = p.nachname
        self.nested_name = { 'first': p.vorname, 'last': p.nachname }
        # ~2% should be diverse
        self.gender = 'diverse' if randint(0,100) < 2 else ('male' if p.geschlecht else 'female')
        self.birthname = p.geburtsname if p.geburtsname != p.nachname else None 

    def to_dict(self):
        return vars(self)

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4, ensure_ascii=False)

    def to_xml(self):
        return dicttoxml(self.to_dict())

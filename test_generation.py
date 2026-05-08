'''Stage 1 generation tests — no Elasticsearch required. Run with: pytest test_generation.py'''
import random
import uuid as uuid_module
import pytest
from faker import Faker

from load_all_types_random import document_stream

SEED = 42
N_DOCS = 20

REQUIRED_FIELDS = [
    'uuid', 'num_id', 'created_at', 'firstname', 'lastname', 'nested_name',
    'birthplace', 'nickname', 'gender', 'date_of_birth', 'email_address',
    'ip_address', 'lefthanded', 'address_st', 'address_no',
    'city_location', 'neighborhood', 'postal_code', 'state',
    'phone_number', 'marital_status', 'num_children',
    'person_xml', 'person_json',
    'some_const_keyword', 'some_text_without_multi_field',
    'some_text_with_array_multifield_content', 'some_text_with_ignored_keyword',
    'some_epoch_date', 'some_bool', 'some_binary',
    'some_long', 'some_int', 'some_short', 'some_byte',
    'some_unsigned_long', 'some_double', 'some_float',
    'some_half_float', 'some_scaled_float',
]


@pytest.fixture(scope='module')
def docs():
    random.seed(SEED)
    Faker.seed(SEED)
    return [doc['_source'] for doc in document_stream('test-idx', N_DOCS)]


def test_field_presence(docs):
    for doc in docs:
        missing = [f for f in REQUIRED_FIELDS if f not in doc]
        assert not missing, f'Missing fields: {missing}'


def test_age_absent(docs):
    for doc in docs:
        assert 'age' not in doc


def test_uuid_valid(docs):
    for doc in docs:
        uuid_module.UUID(doc['uuid'])  # raises ValueError if not a valid UUID


def test_uuid_unique(docs):
    uuids = [doc['uuid'] for doc in docs]
    assert len(set(uuids)) == len(uuids)


def test_num_id_sequence(docs):
    assert [doc['num_id'] for doc in docs] == list(range(1, N_DOCS + 1))


def test_gender_values(docs):
    for doc in docs:
        assert doc['gender'] in {'male', 'female', 'diverse'}


def test_marital_status_values(docs):
    for doc in docs:
        assert doc['marital_status'] in {'single', 'married', 'divorced', 'widowed'}


def test_num_children_range(docs):
    for doc in docs:
        assert 0 <= doc['num_children'] <= 5


def test_geo_point_in_germany(docs):
    for doc in docs:
        loc = doc['city_location']
        assert 'lat' in loc and 'lon' in loc
        assert 47.0 <= loc['lat'] <= 56.0, f"lat out of bounds: {loc['lat']}"
        assert 5.5 <= loc['lon'] <= 15.5, f"lon out of bounds: {loc['lon']}"


def test_geo_shape_polygon(docs):
    for doc in docs:
        nb = doc['neighborhood']
        assert nb['type'] == 'Polygon'
        ring = nb['coordinates'][0]
        # 0–4 corner notches → 4/6/8/10/12 edges → 5/7/9/11/13 ring entries (incl. closing vertex)
        assert len(ring) in {5, 7, 9, 11, 13}, f'Unexpected ring length: {len(ring)}'
        assert ring[0] == ring[-1], 'Ring not closed'
        # All vertices must be right angles (rectilinear polygon)
        n = len(ring) - 1
        for j in range(n):
            ax, ay = ring[j]
            bx, by = ring[(j + 1) % n]
            cx, cy = ring[(j + 2) % n]
            dot = (bx - ax) * (cx - bx) + (by - ay) * (cy - by)
            assert abs(dot) < 1e-8, f'Vertex {j + 1} is not a right angle (dot={dot})'


def test_some_const_keyword(docs):
    for doc in docs:
        assert doc['some_const_keyword'] == 'random'


def test_nested_name_consistent(docs):
    for doc in docs:
        assert doc['nested_name']['first'] == doc['firstname']
        assert doc['nested_name']['last'] == doc['lastname']


def test_reproducibility(docs):
    '''Same seed produces identical documents (UUIDs excepted — time-based).'''
    random.seed(SEED)
    Faker.seed(SEED)
    docs2 = [doc['_source'] for doc in document_stream('test-idx', N_DOCS)]
    check_fields = ['firstname', 'lastname', 'date_of_birth', 'city_location',
                    'marital_status', 'num_children', 'postal_code', 'state']
    for d1, d2 in zip(docs, docs2):
        for field in check_fields:
            assert d1[field] == d2[field], f'Field {field!r} differs between seeded runs'

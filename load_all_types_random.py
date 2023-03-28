from random import randint, getrandbits, uniform
import base64
import random_person as rp


# create document generator
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

def main():
    ''' main function '''
    for person_dict in document_stream('all_types_random-1', 10):
        print(person_dict)

if __name__ == "__main__":
    main()

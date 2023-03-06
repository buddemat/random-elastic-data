import uuid
from datetime import datetime

class RandomPerson:
    """Class for random person"""
    _instance_count = 0

    def __init__(self, uuid=str(uuid.uuid1())):
        RandomPerson._instance_count += 1
        self.created_at = datetime.today() # created_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        self.uuid = uuid
        self.num_id = RandomPerson._instance_count



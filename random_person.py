import uuid
from datetime import datetime

class RandomPerson:
    """Class for random person"""
    def __init__(self):
        self.created_at = datetime.today() # created_at.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        self.uuid = str(uuid.uuid1())



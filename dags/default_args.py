from datetime import date
from  datetime import  datetime
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024,6, 1)
}

kafka_args = {
    'KAFKA_BOOTSTRAP_SERVERS': ["broker:29092"],
    "KAFKA_TOPIC_USERS_CREATED": "user_created",
    "MAX_BLOCK_MS": 5000
}

print(date.today())
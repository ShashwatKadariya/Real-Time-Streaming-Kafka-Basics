from datetime import  date, datetime
import logging
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from default_args import default_args, kafka_args
import json
import requests
import uuid
from kafka import KafkaProducer
from UUIDEncoder import UUIDEncoder
from airflow.utils.dates import days_ago

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def get_data(custom_data_format):
    try:
        res = requests.get("https://randomuser.me/api/").json()['results'][0]
        return custom_data_format(res)
    except requests.exceptions.RequestException as e:
        print("Error fetching Data: ", e)


def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=kafka_args['KAFKA_BOOTSTRAP_SERVERS'],
        max_block_ms=kafka_args["MAX_BLOCK_MS"],
    )
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:  # 1 min
            break
        try:
            res = get_data(format_data)
            producer.send(kafka_args['KAFKA_TOPIC_USERS_CREATED'], json.dumps(res, cls=UUIDEncoder).encode('utf-8'))
        except Exception as e:
            logging.error("An error occurred: ", e)
            continue


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='*/2 * * * *',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

import producer_server
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import time

TOPIC_NAME = "com.udacity.project.policeCalls"  #"RadioCode"
BROKER_URL = "localhost:9092"


def run_kafka_server():
    # TODO get the json file path
    input_file = "police-department-calls-for-service.json"
    print("before producer")
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=TOPIC_NAME,
        bootstrap_servers=BROKER_URL,
        client_id="radioCodeId"
    )
    

    print("after producer")

    return producer


        
def feed():
    producer = run_kafka_server()
    print("in feed method")
    print(producer)
    producer.generate_data()



if __name__ == "__main__":
    feed()


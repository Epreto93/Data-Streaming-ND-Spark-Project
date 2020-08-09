import asyncio
from dataclasses import dataclass, field
import json

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic



BROKER_URL = "PLAINTEXT://localhost:9092"
TopicName = "com.udacity.project.policeCalls" 


async def consume(topic_name):
    print("in consome method")
    print("topic name")
    print(topic_name)
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0","auto.offset.reset":"beginning"})
    c.subscribe([topic_name])

    while True:
    
        print("before consume")
        messages = c.consume(5,1)
        print("messages: ")
        print(messages)
        print("after consume")
        for message in messages:
            if message is None:
                print("in none")
                print("no message")
            elif message.error() is not None:
                print("error")
            else:
                print("in else")
                print(f"key: {message.key()}, value: {message.value()}")

        await asyncio.sleep(0.01)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(produce_consume(TopicName))
    except KeyboardInterrupt as e:
        print("shutting down")




async def produce_consume(topic_name): 


    t1 = asyncio.create_task(consume(topic_name))
    await t1


if __name__ == "__main__":
    main()
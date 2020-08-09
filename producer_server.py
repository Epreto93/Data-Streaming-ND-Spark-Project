from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):  # KafkaProducer

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        

    #TODO we're generating a dummy data
    def generate_data(self):
        print("in generate data")
 
        print("in while loop")
        with open(self.input_file) as f:
            print("read from file")
            data = json.load(f)
            print("data is: " + str(data))
            for line in data:
                message = self.dict_to_binary(line)
                print("new message"+str(message))
                # TODO send the correct data
                print(self.topic)
                result = self.send(self.topic,message)
                print("this is result: ")
                print(result)
                time.sleep(1)
                
                    

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        newData = json.dumps(json_dict).encode('utf-8')
        return newData

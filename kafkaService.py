from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient,NewTopic
from mongoService import Mongo
import json
import pandas as pd
import time

class Admin():
    def __init__(self, kafka_server_url:str) -> None:
        self.admin = KafkaAdminClient(bootstrap_servers=kafka_server_url)        

    def createNewTopic(self, topic_name:str, number_of_partitions:int=2, replication_factor:int=1):
        try:
            self.admin.create_topics([NewTopic(name=topic_name,num_partitions=number_of_partitions,replication_factor=replication_factor)])
            return f"Successfully Created topic: {topic_name}"
        except Exception as e:
            return f"Failed to create the topic: {topic_name} and occurred error is: {e}"
                

    def listTopics(self):
        return self.admin.list_topics()
    

    def deleteTopic(self, topic_name:str):
        try:
            self.admin.delete_topics([topic_name])
            return f"Successfully deleted topic: {topic_name}"
        except Exception as e:
            return f"Failed to delete the topic: {topic_name} and occurred error is: {e}"
        

    def closeConnection(self):
        try:
            self.admin.close()
            print("Successfully closed the connection for Admin client") 
        except:
            print("Failed to close the connection for Admin client")

    

class Producer():
    def __init__(self, kafka_server_url:str) -> None:
        self.producer = KafkaProducer(bootstrap_servers=kafka_server_url,value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    @staticmethod
    def __csvToListOfRecords(file:str)->list:
        """
        This functions read the .csv file and returns list of dictionary of records which can be used to dump data in kafka producer
        Input:
        file (str): path of csv file
        Output:
        list: list of dictionary of records which can be passed one by one in producer
        """
        df = pd.read_csv(file)
        columns = {col:col.strip() for col in df.columns}
        df = df.rename(columns=columns)
        return df.to_dict(orient='index').values()
    

    def produce(self,topics:list, files:list):
        for topic, file in zip(topics,files):
            data = self.__csvToListOfRecords(file)
            for record in data:
                self.producer.send(topic=topic,value=record)
            else:
                print(f"Successfully published {len(data)} records in {topic}")

    def closeConnection(self):
        try:
            self.producer.close()
            print("Successfully closed the connection for Producer client") 
        except:
            print("Failed to close the connection for Producer client")



class Consumer(Mongo):

    def __init__(self, kafka_server_url:str, topic_name:str, mongo_database) -> None:
        super().__init__("root","connect")
        self.topic = topic_name
        self.db = mongo_database
        self.consumer = KafkaConsumer(bootstrap_servers=kafka_server_url,\
                                      value_deserializer=lambda m: json.loads(m.decode('utf-8')),\
                                        auto_offset_reset='earliest', enable_auto_commit=False,\
                                            group_id = topic_name,consumer_timeout_ms=15000)
        
        self.__consume()

    def __consume(self):
        self.consumer.subscribe([self.topic])
        print("Started the consumption of messages from topic: ",self.topic)
        fetched_data = []
        total_records = 0
        for message in self.consumer:
            value = message.value
            fetched_data.append(value)

            self.consumer.commit()

            if len(fetched_data)>=100:
                print(f"time: {time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())} and consumed data count: ",len(fetched_data))
                pass
                # upload data in mongoDB and empty the fetched_data
                msg = self.addRecords(self.db,self.topic,fetched_data)
                total_records += len(fetched_data)
                fetched_data = []

        if fetched_data:
            print(f"time: {time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())} and consumed **last batch** data count: ",len(fetched_data))
            # upload data in mongoDB and empty the fetched_data
            msg = self.addRecords(self.db,self.topic,fetched_data)
            total_records += len(fetched_data)
            fetched_data = []

        self.consumer.close()
        print(f"Successfully consumed all the messages ({total_records}) from topic: {self.topic} and closed the connection with broker")
            



topics = ["case","region","time-province"]
files = ["./files/Case.csv","./files/Region.csv","./files/TimeProvince.csv"]

# Creating topics
admin =  Admin("localhost:9094")

for topic in topics:
    resp = admin.createNewTopic(topic)
    print(resp)

print("topics list: ",admin.listTopics())

admin.closeConnection()
        

# Producing the messages in topics
producer = Producer("localhost:9094")
producer.produce(topics,files)
producer.closeConnection()


# Consuming the data from topics
for topic in topics:
    consumer = Consumer("localhost:9094",topic,"covid")


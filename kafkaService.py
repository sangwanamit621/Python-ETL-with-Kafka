from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from mongoService import Mongo
import json
import pandas as pd
import time

class Admin():
    """
    In this class, we will define functions to create, list and delete a topic from Kafka broker
    """
    def __init__(self, kafka_server_url:str) -> None:
        self.admin = KafkaAdminClient(bootstrap_servers=kafka_server_url)        

    def createNewTopic(self, topic_name:str, number_of_partitions:int=2, replication_factor:int=1):
        """
        We will create a topic in Kafka Broker with given specifications and print the status of operation.
        Input:
        topic_name: Name of Topic which we want to create in the Kafka broker
        number_of_partitions: Number of Partitions we want to create in a topic. This will define how many consumers of same group can consume the data
        replication_factor: Number of copies of data to store in different kafka brokers to allow Kafka to provide high availability of data and prevent data loss if the broker goes down or cannot handle the request
        """
        try:
            self.admin.create_topics([NewTopic(name=topic_name,num_partitions=number_of_partitions,replication_factor=replication_factor)])
            return f"Successfully Created topic: {topic_name}"
        except Exception as e:
            return f"Failed to create the topic: {topic_name} and occurred error is: {e}"
                

    def listTopics(self):
        """Returns the list of names of topics present in a Kafka Broker"""
        return self.admin.list_topics()
    

    def deleteTopic(self, topic_name:str):
        """
        We will delete a topic from Kafka Broker and print the status of operation.
        Input:
        topic_name: Name of Topic which we want to delete from the Kafka broker
        """
        try:
            self.admin.delete_topics([topic_name])
            return f"Successfully deleted topic: {topic_name}"
        except Exception as e:
            return f"Failed to delete the topic: {topic_name} and occurred error is: {e}"
        

    def closeConnection(self):
        """
        To close the connection of Admin Client with the broker after performing all the operations 
        """
        try:
            self.admin.close()
            print("Successfully closed the connection for Admin client") 
        except:
            print("Failed to close the connection for Admin client")

    

class Producer():
    """
    In this class, we have defined different methods to publish the records after reading data from a file
    """
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
        """
        To publish/send the records from a file into the topic of Kafka broker. After successfully publishing the records, prints the message with total number of records published.
        Input:
        topics: List of topics in which we want to publish the records
        files: List of files from which we want to publish the records        
        """
        for topic, file in zip(topics,files):
            data = self.__csvToListOfRecords(file)
            for record in data:
                self.producer.send(topic=topic,value=record)
            else:
                print(f"Successfully published {len(data)} records in {topic}")

    def closeConnection(self):
        """
        To close the connection of Producer Client with the broker after performing all the operations.
        """
        try:
            self.producer.close()
            print("Successfully closed the connection for Producer client") 
        except:
            print("Failed to close the connection for Producer client")



class Consumer(Mongo):
    """
    In this class, we have performed operation to consume the data and dump it in the collection of mongoDB database
    """
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
        """
        In this function, we have subscribed to the topic, consumed the data and then dumped the data in mongoDB collection. If consumer will not recieve any data for 15 seconds then connection with broker will be closed.
        """
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

# Creating topics using Admin class
admin =  Admin("localhost:9094")

for topic in topics:
    resp = admin.createNewTopic(topic)
    print(resp)

print("topics list: ",admin.listTopics())

admin.closeConnection()
        

# Producing the messages in topics using Producer class
producer = Producer("localhost:9094")
producer.produce(topics,files)
producer.closeConnection()


# Consuming the data from topics and dumping data in the mongoDB
for topic in topics:
    consumer = Consumer("localhost:9094",topic,"covid")


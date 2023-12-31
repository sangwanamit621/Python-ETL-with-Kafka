# About the Project
In this project, we will read data from a file and will produce and consume the data from kafka running on docker container. After consumption we will store the data in mongoDB. The purpose of this project is to provide a scalable and efficient way to load and process data from multiple sources into a single data store.

## Tech Stacks
* Python
* Kafka
* Docker
* MongoDB

## Project Flow
1. Extract the data from the source files.
2. Transform the data into a consistent format.
3. Load the data into Apache Kafka.
4. Consume the data from Apache Kafka.
5. Load the data into MongoDB.
   ![Screenshot from 2023-09-19 19-47-23](https://github.com/sangwanamit621/kafka-etl/assets/96620780/d32ffeae-e40c-492e-9784-cbaf34138702)

## Tasks Performed
1. Setup the Kafka cluster and MongoDB Cluster using docker (We have used docker-compose to set up the clusters over a custom defined network)
2. Establish connection with Kafka broker running on docker container
3. Use Python to create topics using Kafka AdminClient in the Kafka Broker
4. Read data from different files and send the data in respective topic using Kafka Producer
5. Consume the data from different topics using Kafka Consumer
6. Establish connection with MongoDB cluster running on docker container
7. Create a database and dump the data of each topic in respective collection




# About the Project
In this project, we will read data from a file and will produce and consume the data from kafka running on docker container. After consumption we will store the data in mongoDB.

## Tech Stacks
* Python
* Kafka
* Docker
* MongoDB

## Project Flow
1. Read data from the file.
2. Send this data in the respective topic of kafka using Kafka Producer
3. Consume the data from respective topic
4. Dump the consumed data in the mongoDB so that we can use the data to perform other required operations

## Tasks Performed
1. Setup the Kafka cluster and MongoDB Cluster using docker (We have used docker-compose to set up the clusters over a custom defined network)
2. Establish connection with Kafka broker running on docker container
3. Use Python to create topics using Kafka AdminClient in the Kafka Broker
4. Read data from different files and send the data in respective topic using Kafka Producer
5. Consume the data from different topics using Kafka Consumer
6. Establish connection with MongoDB cluster running on docker container
7. Create a database and dump the data of each topic in respective collection



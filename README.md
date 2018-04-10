# Kafka MongoDB Connector
This connector is used to connect Mongo DB to kafka (Source) and Kafka to Mongo DB (Sink).

# What's new here ?
Schema registry is not required to sink data's into Mongo DB.

# Build
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```


# Mongo Source Connector
If the connector is running as a Source Connector, it reads data from Mongodb oplog (https://docs.mongodb.org/manual/core/replica-set-oplog/) and publishes it on Kafka as a JSON String.

## Sample Configuration
```ini
name=mongo-source
connector.class=com.kafka.mongo.connector.MongoSourceConnector
tasks.max=1
host=localhost
uri=mongodb://127.0.0.1:27017
batch.size=1
schema.name=test
databases=test.contacts
port=27017
topic.prefix=
delete=false
delete.toipics=false
```

* **name**: name of the connector
* **connector.class**: class of the implementation of the connector
* **tasks.max**: maximum number of tasks to create
* **host**: mongodb host (required if uri is not informed)
* **uri**: mongodb uri (required if host is not informed)
* **batch.size**: maximum number of messages to write on Kafka at every poll() call
* **schema.name**: name to use for the schema, it will be formatted as ``{schema.name}_{database}_{collection}``
* **databases**: comma separated list of collections from which import data
* **port**: mongodb port (required if uri is not informed) 
* **topic.prefix**: optional prefix to append to the topic names. The topic name is formatted as ``{topic.prefix}_{database}_{collection}``
* **delete**: If true, Adding delete filter to mongo, which will fetch delete records.
* **delete.toipics**: If true, Create a new kafka topic for the records which are going to delete from mongoDB. (eg: kafka topic name as per configuration testdb_contacts_delete)

# Mongo Sink Connector
Once the connector is run as Sink, it retrieves messages from Kafka as JSON String with escape quotes and writes them on mongodb collections. 

## Sample Configuration
```ini
name=mongo-sink
connector.class=com.kafka.mongo.connector.MongoSinkConnector
tasks.max=1
uri=mongodb://127.0.0.1:27017
bulk.size=100
mongodb.collections=mytest
mongodb.database=test
username=admin
password=admin
authSource=admin
port=27017
host=localhost
topics=sink_test
```
* **name**: name of the connector
* **connector.class**: class of the implementation of the connector
* **tasks.max**: maximum number of tasks to create
* **uri**: mongodb uri (required if host is not informed)
* **bulk.size**: maximum number of documents to write on Mongodb at every put() call
* **mongodb.collections**: comma separated list of collections on which write the documents
* **mongodb.database**: database to use
* **username**: mongodb username
* **password**: mongodb password
* **authSource**: authSource scope is not utilized, Hence given value as admin.
* **port**: mongodb port 
* **host**: mongodb host 
* **topics**: comma separated list of topics to write on Mongodb

The number of collections and the number of topics should be same.

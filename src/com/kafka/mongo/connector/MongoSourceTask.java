package com.kafka.mongo.connector;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.bson.BsonTimestamp;
import org.bson.Document;

import com.kafka.mongo.reader.MongoDBReader;

/**
 * 
 * @author Sujith Ramanathan
 *
 */

public class MongoSourceTask extends SourceTask {
	
    private Integer port;
    private String host;
    private String schemaName;
    private List<String> databases;
    private int batchSize;
    private String delete;
    private String deleteTopics;
    private static Map<String, Schema> schemas = null;
    
    
    private MongoDBReader reader;
    
    Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<Map<String, String>, Map<String, Object>>(0);

	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		try{
			port = Integer.parseInt(map.get(MongoSourceConnector.PORT));
			batchSize  = Integer.parseInt(map.get(MongoSourceConnector.BATCH_SIZE));
		}catch(ConnectException ce){
			ce.printStackTrace();
		}
		System.out.println("map.toString() "+map.toString());
        schemaName = map.get(MongoSourceConnector.SCHEMA_NAME);
        host = map.get(MongoSourceConnector.HOST);        
        databases = Arrays.asList(map.get(MongoSourceConnector.DATABASES).split(","));
        batchSize = Integer.parseInt(map.get(MongoSourceConnector.BATCH_SIZE));
        delete = map.get(MongoSourceConnector.DELETE);
        deleteTopics = map.get(MongoSourceConnector.DELETE_TOPICS);        
        
        if(null==schemas){
        	schemas = new HashMap<String,Schema>();
        }
        
        for(String db : databases){
        	db = db.replaceAll("[\\s.]", "_");
        	if(null==schemas.get(db)){
        		schemas.put(db, SchemaBuilder.struct()
        									 .name(schemaName.concat("_").concat(db))
        									 .field("timestamp",Schema.OPTIONAL_INT32_SCHEMA)
        									 .field("order",Schema.OPTIONAL_INT32_SCHEMA)
        									 .field("operation",Schema.OPTIONAL_STRING_SCHEMA)
        									 .field("database",Schema.OPTIONAL_STRING_SCHEMA)
        									 .field("object",Schema.OPTIONAL_STRING_SCHEMA)
        									 .build()
        					);
        	}
        }
        loadOffsets();
        reader = new MongoDBReader(host, port, databases,offsets,batchSize,delete);
        reader.run();
	}
	
	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		List<SourceRecord> records = new ArrayList<SourceRecord>(0);
		Document message = null;
		Struct messageStruct;
		String topic;
		String db;
		String timeStamp;
		while(!reader.messages.isEmpty() && records.size()<batchSize){
			message = reader.messages.poll();
			messageStruct = getStruct(message);
			topic = getTopic(message);
			db = getDB(message);
			timeStamp = getTimestamp(message);
			records.add(new SourceRecord(Collections.singletonMap("mongodb", db),Collections.singletonMap(db, timeStamp), topic, messageStruct.schema(), messageStruct));
		}
		return records;
	}


	@Override
	public void stop() {
		
	}
	
	private Struct getStruct(Document message){
		Schema schema = schemas.get(getDB(message).replaceAll("[\\s.]","_"));
		Struct messageStruct = new Struct(schema);
		BsonTimestamp bsonTimeStamp = (BsonTimestamp)message.get("ts");
		Integer seconds = bsonTimeStamp.getTime();
		Integer order = bsonTimeStamp.getInc();
		messageStruct.put("timestamp",seconds);
		messageStruct.put("order",order);
		messageStruct.put("operation",message.get("op"));
		messageStruct.put("database",message.get("ns"));
		messageStruct.put("object",message.get("o").toString());
		return messageStruct;
	}
	
	private String getTopic(Document message){
		String database = ((String) message.get("ns")).replaceAll("[\\s.]", "_");
		boolean delOperation = message.get("op").equals("d");
		
		if(delOperation && "true".equalsIgnoreCase(deleteTopics)){
			database=database.concat("_delete");
		}
		
		return database;
	}
	
    private String getTimestamp(Document message) {
        BsonTimestamp timestamp = (BsonTimestamp) message.get("ts");
        return new StringBuilder()
                .append(timestamp.getTime())
                .append(timestamp.getInc())
                .toString();
    }
	
	private String getDB(Document message){
		return (String)message.get("ns");
	}

    private void loadOffsets() {
        List<Map<String, String>> partitions = new ArrayList<Map<String, String>>();
        Map<String, String> partition = null;
        for (String db : databases) {
        	partition = Collections.singletonMap("mongodb", db);
            partitions.add(partition);
        }
        offsets.putAll(context.offsetStorageReader().offsets(partitions));
    }
}
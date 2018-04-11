package com.kafka.mongo.connector;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

/**
 * @author Sujith Ramanathan
 */

@SuppressWarnings({"unused","rawtypes","resource","unchecked"})
public class MongoSinkTask extends SinkTask{

    private String userName;
    private String pwd;
    private String authSource;
    private Integer port;
    private String host;
    private Integer bulkSize;
    private String collections;
    private String database;
    private String topics;
    
	private Map<String, MongoCollection> mapping;
	private MongoDatabase mongoDB;
	
	private ObjectMapper mapper;
	
	Logger logger = Logger.getLogger(MongoSinkTask.class);
    
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		try{
			port = Integer.parseInt(map.get(MongoSinkConnector.PORT));
			mapper = new ObjectMapper();
		}catch(Exception e){
			try {
				throw new ConnectException("port must be a numeric value ");
			} catch (ConnectException ce) {
				ce.printStackTrace();
				System.exit(0);
			}
		}
		try{
			bulkSize = Integer.parseInt(map.get(MongoSinkConnector.BULK_SIZE));
		}catch(Exception e){
			try {
				throw new ConnectException("bulk.size must be a numeric value ");
			} catch (ConnectException ce) {
				ce.printStackTrace();
				System.exit(0);
			}
		}
		userName = map.get(MongoSinkConnector.USERNAME);
		pwd = map.get(MongoSinkConnector.PASSWORD);
		authSource = map.get(MongoSinkConnector.AUTH_SOURCE);
		database = map.get(MongoSinkConnector.DATABASE);
		host = map.get(MongoSinkConnector.HOST);
		collections = map.get(MongoSinkConnector.COLLECTIONS);
		topics = map.get(MongoSinkConnector.TOPICS);
		
        List<String> collectionsList = Arrays.asList(collections.split(","));
        List<String> topicsList = Arrays.asList(topics.split(","));
        
		MongoClient mongoClient = new MongoClient(host,port);
        mongoDB = mongoClient.getDatabase(database);
        mapping = new HashMap<String,MongoCollection>();
        String topics;
        String collection;
        for(int i=0;i<topicsList.size();i++){
        	topics = topicsList.get(i);
        	collection = collectionsList.get(i);
        	mapping.put(topics,mongoDB.getCollection(collection));
        }
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		String idKey;
		List<SinkRecord> recordList = new ArrayList<SinkRecord>(records);
		SinkRecord record;
		Map<String,List<WriteModel<Document>>> bulks;
		Map<String,Object> jsonMap;
		String topic;
		Document newDocument;
		for(int i=0;i<recordList.size();i++){
			bulks = new HashMap<String,List<WriteModel<Document>>>();
			for(int j=0;j<bulkSize && i<recordList.size();i++,j++){
				record = recordList.get(i);
				idKey = String.valueOf(record.key());
				try {
					jsonMap = (Map<String,Object>)mapper.readValue((String)record.value(), HashMap.class);
					topic = record.topic();
					if(null==bulks.get(topic)){
						bulks.put(topic, new ArrayList<WriteModel<Document>>());
					}
					if(null!=jsonMap){
						newDocument = new Document(jsonMap).append("_id",idKey);
						bulks.get(topic).add(new UpdateOneModel<Document>(Filters.eq("_id",idKey),new Document("$set",newDocument),new UpdateOptions().upsert(true)));
					}
				} catch (Exception e) {
					logger.warn("Unable to parse the message : ".concat((String)record.value()), e);
					continue;
				}
			}
			i--;
			for(String key : bulks.keySet()){
				try{
//					BulkWriteResult result = mapping.get(key).bulkWrite(bulks.get(key));
					mapping.get(key).bulkWrite(bulks.get(key));
				}catch(Exception e){
					logger.error(e.getMessage());
				}
			}
		}
	}
	
    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

}

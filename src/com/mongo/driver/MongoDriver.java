package com.mongo.driver;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

public class MongoDriver {

	private static Datastore dataStore;
	
	public static Datastore getDataStore(){
		if(null==dataStore)
			dataStore = connectToDB();
		
		return dataStore;
	}
	
	private static Datastore connectToDB(){
		MongoClient mongoClient = null;
		MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
		builder.connectionsPerHost(50).codecRegistry(com.mongodb.MongoClient.getDefaultCodecRegistry());
		builder.threadsAllowedToBlockForConnectionMultiplier(10);
		builder.maxWaitTime(30000);
		builder.connectTimeout(20000);
		builder.socketTimeout(60000);
		mongoClient = new MongoClient("localhost",27017);
		Morphia morphia = new Morphia();
		morphia.mapPackage("com.mongo.entity");
		Datastore dataStore = morphia.createDatastore(mongoClient, "Test");//DB Name
		dataStore.ensureIndexes();
		return dataStore;
	}
}

package com.kafka.mongo.reader;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

/**
 * 
 * @author Sujith Ramanathan
 *
 */

public class DBReader implements Runnable{

	private String host;
	private int port;
	private String db;
	private String start;
	private ConcurrentLinkedQueue<Document> messages;
	private MongoCollection<Document> oplog;
	private Bson query;
	private int page = 0;
	private int batchSize = 0;
	private String delete = "";
	
	private MongoClient client;

	public DBReader(String host,int port,String db,String start,int batchSize,ConcurrentLinkedQueue<Document> messages,String delete) {
		this.host=host;
		this.port=port;
		this.db=db;
		this.start=start;
		this.batchSize=batchSize;
		this.messages=messages;
		this.delete=delete;
		init();
	}
	
	public DBReader(){
		
	}
	
	@SuppressWarnings("unchecked")
	private void init(){
		oplog = readCollection();
		query = createQuery();
	}

	public void run() {
		FindIterable<Document> documents;
		while(true){
			if(messages.isEmpty()){
				try{
					documents = find(page);
					for(Document doc : documents){
						messages.add(doc);
						System.out.println(doc.toJson());
					}
				}catch(Exception e){
					e.printStackTrace();
				}
				page++;
			}else{
				try {
					System.out.println("sleeping");
					Thread.sleep(10000);
				} catch (InterruptedException ie) {
					ie.printStackTrace();
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	private MongoCollection readCollection(){
		client = new MongoClient(host,port);
		MongoDatabase db = client.getDatabase("local");
		MongoCollection<Document> collection = db.getCollection("oplog.rs");
		
		return collection; 
	}
	
	@Override
	public void finalize(){
		if(null!=client)
			client.close();
	}
	
	private Bson createQuery(){
		Long timeStamp = Long.parseLong(start);
		int order = new Long(timeStamp % 10).intValue();
		timeStamp = timeStamp / 10;
		int finalTimeStamp = timeStamp.intValue();
		Bson query = null;
		if("true".equalsIgnoreCase(delete)){
			query = Filters.and(
		
						Filters.exists("fromMigrate",false),
						Filters.gt("ts",new BSONTimestamp(finalTimeStamp,order)),
						Filters.or(
							Filters.eq("op","i"),
							Filters.eq("op","u"),
							Filters.eq("op", "d")
						),
						Filters.eq("ns",db)
					 );
		}else{
			query = Filters.and(
					
					Filters.exists("fromMigrate",false),
					Filters.gt("ts",new BSONTimestamp(finalTimeStamp,order)),
					Filters.or(
						Filters.eq("op","i"),
						Filters.eq("op","u")
					),
					Filters.eq("ns",db)
				 );
		}
		return query;
	}
	
	private FindIterable<Document> find(int page){
		final FindIterable<Document> docs = oplog.find(query)
												 .sort(new Document("$natural",1))
												 .skip(page * batchSize) //page size.
												 .limit(batchSize)
												 .projection(Projections.include("ts","op", "ns", "o"))
												 .cursorType(CursorType.TailableAwait);
		return docs;
	}
	
	public static void main(String []args){
		DBReader reader = new DBReader("localhost", 27017, "test.contacts", "0",100,new ConcurrentLinkedQueue<Document>(),"");
		reader.run();
	}
	
}

// mongod --config /etc/mongod.conf
// mongo --port 27017
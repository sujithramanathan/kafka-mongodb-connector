package com.kafka.mongo.reader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.bson.Document;

/**
 * @author Sujith Ramanathan
 */


public class MongoDBReader {

    public ConcurrentLinkedQueue<Document> messages;

    private List<String> dbs;
    private String host;
    private Integer port;
    private int batchSize;
    private String delete;
    private Map<Map<String, String>, Map<String, Object>> start;
    
    public MongoDBReader(String host, Integer port, List<String> dbs, Map<Map<String, String>, Map<String, Object>> start,int batchSize,String delete) {
        this.host = host;
        this.port = port;
        this.dbs = new ArrayList<String>(0);
        this.dbs.addAll(dbs);
        this.start = start;
        this.messages = new ConcurrentLinkedQueue<Document>();
        this.batchSize = batchSize;
        this.delete = delete;
    }
    
    public void run() {
        // for every database to watch
        for (String db : dbs) {
            String start;
            // get the last message that was read
            Map<String, Object> dbOffset = this.start.get(Collections.singletonMap("mongodb", db));
            if (dbOffset == null || dbOffset.isEmpty())
                start = "0";
            else
                start = (String) this.start.get(Collections.singletonMap("mongodb", db)).get(db);

            // start a new thread for reading mutation of the specific database
            DBReader reader = new DBReader(host, port, db, start,batchSize,messages,delete);
            new Thread(reader).start();
        }
    }
	
}
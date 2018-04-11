package com.kafka.mongo.connector;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.log4j.Logger;

/**
 * @author Sujith Ramanathan
 */

public class MongoSinkConnector extends SinkConnector{

	private static final Logger logger = Logger.getLogger(MongoSinkConnector.class);
	
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String AUTH_SOURCE = "authSource";
    public static final String PORT = "port";
    public static final String HOST = "host";
    public static final String BULK_SIZE = "bulk.size";
    public static final String DATABASE = "mongodb.database";
    public static final String COLLECTIONS = "mongodb.collections";
    public static final String TOPICS = "topics";
    
    private String userName;
    private String pwd;
    private String authSource;
    private String port;
    private String host;
    private String bulkSize;
    private String database;
    private String collections;
    private String topics;
    
    private static final ConfigDef CONFIG_DEF = new ConfigDef().define("MONGO-DB",Type.STRING,Importance.HIGH,"MongoCollection");
    
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		try{
			userName = map.get(USERNAME);
			if(null==userName || userName.isEmpty()){
				throw new ConnectException("Missing username config");
			}
			pwd = map.get(PASSWORD);
			if(null==pwd || pwd.isEmpty()){
				throw new ConnectException("Missing password config");
			}
			authSource = map.get(AUTH_SOURCE);
			if(null==authSource || authSource.isEmpty()){
				throw new ConnectException("Missing authSource config");
			}
			port = map.get(PORT);
			if(null==port || port.isEmpty()){
				throw new ConnectException("Missing port config");
			}
			host = map.get(HOST);
			if(null==host || host.isEmpty()){
				throw new ConnectException("Missing host config");
			}
			bulkSize = map.get(BULK_SIZE);
			if(null==bulkSize || bulkSize.isEmpty()){
				throw new ConnectException("Missing bulkSize config");
			}
			database = map.get(DATABASE);
			if(null==database || database.isEmpty()){
				throw new ConnectException("Missing database config");
			}
			collections = map.get(COLLECTIONS);
			if(null==collections || collections.isEmpty()){
				throw new ConnectException("Missing collections config");
			}
			topics = map.get(TOPICS);
			if(null==topics || topics.isEmpty()){
				throw new ConnectException("Missing topics config");
			}
			logger.debug(map.toString());
		}catch(ConnectException ce){
			ce.printStackTrace();
			System.exit(0);
		}
	}

    /**
     * Returns the task implementation for this Connector
     *
     * @return the task class
     */
	public Class<? extends Task> taskClass() {
		return MongoSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> configs = new ArrayList<Map<String,String>>();
		List<String> coll = Arrays.asList(collections.split(","));
		int numGroups = Math.min(coll.size(), maxTasks);
		List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(coll, numGroups);
		List<String> topicList = Arrays.asList(topics.split(","));
		List<List<String>> topicsGrouped = ConnectorUtils.groupPartitions(topicList, numGroups); 
		
		Map<String,String> config;
		for(int i=0;i< numGroups;i++){
			config = new HashMap<String,String>();
			config.put(USERNAME,userName);
			config.put(PASSWORD,pwd);
			config.put(AUTH_SOURCE,authSource);
			config.put(PORT,port);
			config.put(HOST,host);
			config.put(BULK_SIZE,bulkSize);
			config.put(DATABASE,database);
			config.put(COLLECTIONS,join(dbsGrouped.get(i), ","));
			config.put(TOPICS,join(topicsGrouped.get(i), ","));
			configs.add(config);
		}
		return configs;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}
	
    private static <T> String join(Iterable<T> elements, String delim) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (T elem : elements) {
            if (first) {
                first = false;
            } else {
                result.append(delim);
            }
            result.append(elem);
        }
        return result.toString();
    }

}

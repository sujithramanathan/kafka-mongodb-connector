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
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;

/**
 * 
 * @author Sujith Ramanathan
 *
 */

public class MongoSourceConnector extends SourceConnector{

	public static final String PORT="port";
	public static final String HOST="host";
	public static final String SCHEMA_NAME="schema.name";
	public static final String BATCH_SIZE="batch.size";
	public static final String TOPIC_PREFIX="topic.prefix";
	public static final String DATABASES="databases";
	public static final String DELETE="delete";
	public static final String DELETE_TOPICS="delete.toipics";
	
    private String port;
    private String host;
    private String schemaName;
    private String batchSize;
    private String topicPrefix;
    private String databases;
    private String delete;
    private String deleteTopics;
	
    private static final ConfigDef CONFIG_DEF = new ConfigDef().define("Source", Type.STRING, Importance.HIGH, "Topic");
    
    
	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void start(Map<String, String> configMap) {
		System.out.println("Inside start() and validating configMap");
		try{
			port = configMap.get(PORT);
			if(null==PORT || PORT.isEmpty())
				throw new ConnectException("Missing "+PORT+" config");
			schemaName = configMap.get(SCHEMA_NAME);
			if(null==schemaName || schemaName.isEmpty())
				throw new ConnectException("Missing "+SCHEMA_NAME+" config");
			batchSize = configMap.get(BATCH_SIZE);
			if(null==batchSize || batchSize.isEmpty())
				throw new ConnectException("Missing "+BATCH_SIZE+" config");
			host = configMap.get(HOST);
			if(null==host || host.isEmpty())
				throw new ConnectException("Missing "+HOST+" config");
			databases = configMap.get(DATABASES);
			topicPrefix = configMap.get(TOPIC_PREFIX);
			delete = configMap.get(DELETE);
			deleteTopics = configMap.get(DELETE_TOPICS);
			
		}catch(ConnectException ce){
			ce.printStackTrace();
			System.exit(1);
		}
		
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		ArrayList<Map<String, String>> config = new ArrayList<Map<String, String>>();
		List<String> dbs = Arrays.asList(databases.split(","));
		int numGroups = Math.min(dbs.size(),maxTasks);
		List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(dbs, numGroups);
		Map<String,String> configPerGroup=null;
		for(int i=0;i<numGroups;i++){
			configPerGroup = new HashMap<String,String>();
			configPerGroup.put(PORT,port);
			configPerGroup.put(HOST,host);
			configPerGroup.put(SCHEMA_NAME, schemaName);
			configPerGroup.put(BATCH_SIZE, batchSize);
			configPerGroup.put(TOPIC_PREFIX, topicPrefix);
			configPerGroup.put(DATABASES, join(dbsGrouped.get(i), ","));
			configPerGroup.put(DELETE,delete);
			configPerGroup.put(DELETE_TOPICS,deleteTopics);
			config.add(configPerGroup);
		}
		return config;
	}
	
	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

	@Override
	public Class<? extends Task> taskClass() {
		return MongoSourceTask.class;
	}

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}
	
    private <T> String join(Iterable<T> elements, String delim) {
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

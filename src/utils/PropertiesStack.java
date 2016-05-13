package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertiesStack {

	private static final String FILE_NAME = "params.properties";
	private static final Properties properties;
	static {
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(FILE_NAME)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private PropertiesStack() {
	}

	public static String getKafkaBootstrapServers() {
		return properties.getProperty("bootstrap.servers");
		// return "192.168.74.39:9092,192.168.74.40:9092,192.168.74.41:9092";
	}

	public static String getZookeeperConnect() {
		// return "192.168.74.39:2181,192.168.74.40:2181,192.168.74.41:2181";
		return properties.getProperty("zookeeper.connect");
	}

	public static String getKafkaGroupId() {
		return properties.getProperty("group.id");
	}

	public static String getKafkaTopic() {
		return properties.getProperty("kafka.topic");
	}

	public static String getResultKafkaTopic() {
		return properties.getProperty("kafka.result.topic");
	}
	public static Long getKafkaBatchSize() {
		Long batchSize = Long.parseLong(properties.getProperty("kafka.batch.size"));
		return batchSize == -2 ? null:batchSize; 
	}
	

	public static Boolean isCheckpointEnabled() {
		return Boolean
				.parseBoolean(properties.getProperty("checkpoint.enable"));
	}

	public static Long getCheckpointDuration() {
		return Long.parseLong(properties.getProperty("checkpoint.duration"));
	}

	public static Long getFlinkBufferTimeout() {
		Long timeout = Long.parseLong(properties
				.getProperty("flink.buffer.timeout"));
		return timeout == -2 ? null : timeout;
	}
	
	public static Long getFlinkWindowPeriod() {
		return Long.parseLong(properties
				.getProperty("flink.window.period"));
	}

	public static String getHdfsSource() {
		return properties.getProperty("flink.hdfs.source");
	}

	public static String getHdfsSink() {
		return properties.getProperty("flink.hdfs.sink");
	}

	/*
	 * public static String getTwitterAPIKey(){ return
	 * properties.getProperty("twitter.api.key"); }
	 * 
	 * public static String getTwitterAPISecret(){ return
	 * properties.getProperty("twitter.api.secret"); }
	 * 
	 * public static String getTwitterAccessToken(){ return
	 * properties.getProperty("twitter.access.token"); }
	 * 
	 * public static String getTwitterAccessTokenSecret(){ return
	 * properties.getProperty("twitter.access.token.secret"); }
	 */
}

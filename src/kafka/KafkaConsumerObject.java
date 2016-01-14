package kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerObject extends KafkaConsumer<String, String> {

	private static Properties props = new Properties();

	static {
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		//props.put("enable.auto.commit", "true");
		//props.put("auto.commit.interval.ms", "1000");
		//props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("partition.assignment.strategy", "range");
		props.put("auto.offset.reset", "smallest");
	}

	public KafkaConsumerObject() {
		super(props);
	}

	public static void main(String[] args) throws Exception {
		KafkaConsumerObject consumer = new KafkaConsumerObject();
		consumer.subscribe("tweets");
		Map<String, ConsumerRecords<String, String>> records = consumer.poll(100);
		System.out.println(records);
		//System.out.println(records.keySet());
		for (ConsumerRecords<String, String> recordsValues : records.values())
			for (ConsumerRecord<String, String> record : recordsValues.records(0))
				System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());

		consumer.close();
	}

}

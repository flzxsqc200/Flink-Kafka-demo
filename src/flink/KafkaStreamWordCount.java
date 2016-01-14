package flink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import twitter.PropertiesStack;

public class KafkaStreamWordCount {

	public void run() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", PropertiesStack.getKafkaBootstrapServers());
		properties.setProperty("zookeeper.connect", PropertiesStack.getZookeeperConnect());
		properties.setProperty("group.id", PropertiesStack.getKafkaGroupId());
		properties.setProperty("auto.offset.reset", "smallest");

		DataStream<Tuple2<String, Integer>> dataStream = env
				.addSource(new FlinkKafkaConsumer082<>("tweets2", new SimpleStringSchema(), properties))
				.flatMap(new LineSplitter()).keyBy(0).timeWindow(Time.of(10, TimeUnit.SECONDS)).sum(1);

		dataStream.print();

		dataStream.addSink(new SinkFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void invoke(Tuple2<String, Integer> value) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("Key = "+ value.f0+" and value = "+value.f1);
				
			}
		});
		
		env.execute("Window WordCount");
	}

	public static void main(String[] args) {
		try {
			new KafkaStreamWordCount().run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

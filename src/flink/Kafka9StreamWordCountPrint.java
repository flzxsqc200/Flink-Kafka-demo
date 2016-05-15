package flink;

import java.io.Serializable;
import java.sql.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import utils.PropertiesStack;

public class Kafka9StreamWordCountPrint implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = Logger.getLogger(Kafka9StreamWordCountPrint.class);
	static {
		logger.setLevel(Level.ERROR);
	}

	public void run() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		if (PropertiesStack.getFlinkBufferTimeout() != null) {
			env.setBufferTimeout(PropertiesStack.getFlinkBufferTimeout());
		}
		if (PropertiesStack.isCheckpointEnabled())
			env.enableCheckpointing(PropertiesStack.getCheckpointDuration(),
					CheckpointingMode.EXACTLY_ONCE);

		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers",
				PropertiesStack.getKafkaBootstrapServers());
		consumerProperties.setProperty("group.id",
				PropertiesStack.getKafkaGroupId());
		consumerProperties.setProperty("auto.offset.reset", "earliest");

		Properties producerProperties = new Properties();
		producerProperties.setProperty("bootstrap.servers",
				PropertiesStack.getKafkaBootstrapServers());
		producerProperties.setProperty("acks", "all");
		producerProperties.put("retries", Integer.MAX_VALUE);
		producerProperties.setProperty("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.setProperty("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		// producerProperties.setProperty("request.timeout.ms",
		// "60000");
		// producerProperties.setProperty("max.block.ms",
		// "90000");
		if (PropertiesStack.getKafkaBatchSize() != null) {
			producerProperties.setProperty("batch.size", PropertiesStack
					.getKafkaBatchSize().toString());
		}

		DataStream<String> dataStream = env
				// .readFileStream(filePath, intervalMillis, watchType)
				.addSource(
						new FlinkKafkaConsumer09<>(PropertiesStack
								.getKafkaTopic(), new SimpleStringSchema(),
								consumerProperties))
								
				.flatMap(
						new FlatMapFunction<String, Tuple2<String, Integer>>() {
							Random random = new Random();
							@Override
							public void flatMap(String value,
									Collector<Tuple2<String, Integer>> out)
									throws Exception {
								String[] tokens = value.toLowerCase().split("\\W+");
								for (String token : tokens) {
									if (token.length() > 0) {
										out.collect(new Tuple2<String, Integer>(random.nextInt(3)+"WIN-", 1));
									}
								}
							}
						}).keyBy(0)
						.timeWindow(Time.of(PropertiesStack.getFlinkWindowPeriod(), TimeUnit.SECONDS))
				.sum(1).map(new MapFunction<Tuple2<String, Integer>, String>() {
					@Override
					public String map(Tuple2<String, Integer> tuple)
							throws Exception {
						return tuple.f0 + "\t" + tuple.f1;
					}
				});

		dataStream.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value) throws Exception {
				System.out.println(System.currentTimeMillis()+"-> "+value);
				
			}
		});
		env.execute("Window WordCount");
	}

	public static void main(String[] args) {
		try {
			System.out.println("program started at "
					+ System.currentTimeMillis());
			new Kafka9StreamWordCountPrint().run();
		} catch (TimeoutException e) {
			System.out.println(e.getMessage());
			System.out
					.println("program ended at " + System.currentTimeMillis());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

//package flink;
//
//import java.io.Serializable;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.streaming.util.serialization.SerializationSchema;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.kafka.common.errors.TimeoutException;
//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
//
//import utils.PropertiesStack;
//
//public class Kafka82DefaultStreamWordCount implements Serializable {
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;
//	static Logger logger = Logger
//			.getLogger(Kafka82DefaultStreamWordCount.class);
//	static {
//		logger.setLevel(Level.ERROR);
//	}
//
//	public void run() throws Exception {
//		StreamExecutionEnvironment env = StreamExecutionEnvironment
//				.getExecutionEnvironment();
//		if (PropertiesStack.getFlinkBufferTimeout() != null) {
//			env.setBufferTimeout(PropertiesStack.getFlinkBufferTimeout());
//		}
//		if (PropertiesStack.isCheckpointEnabled())
//			env.enableCheckpointing(PropertiesStack.getCheckpointDuration(),
//					CheckpointingMode.EXACTLY_ONCE);
//
//		Properties consumerProperties = new Properties();
//		consumerProperties.setProperty("bootstrap.servers",
//				PropertiesStack.getKafkaBootstrapServers());
//		consumerProperties.setProperty("zookeeper.connect",
//				PropertiesStack.getZookeeperConnect());
//		consumerProperties.setProperty("group.id",
//				PropertiesStack.getKafkaGroupId());
//		consumerProperties.setProperty("auto.offset.reset", "earliest");
//
//		Properties producerProperties = new Properties();
//		producerProperties.setProperty("bootstrap.servers",
//				PropertiesStack.getKafkaBootstrapServers());
//		producerProperties.setProperty("key.serializer",
//				"org.apache.kafka.common.serialization.StringSerializer");
//		producerProperties.setProperty("value.serializer",
//				"org.apache.kafka.common.serialization.StringSerializer");
//		if (PropertiesStack.getKafkaBatchSize() != null) {
//			producerProperties.setProperty("batch.size", PropertiesStack.getKafkaBatchSize().toString());
//		}
//
//		DataStream<String> dataStream = env
//				.addSource(
//						new FlinkKafkaConsumer082<>(PropertiesStack
//								.getKafkaTopic(), new SimpleStringSchema(),
//								consumerProperties))
//				.flatMap(new LineSplitter()).keyBy(0)
//				.timeWindow(Time.of(10, TimeUnit.SECONDS)).sum(1)
//				.map(new MapFunction<Tuple2<String, Integer>, String>() {
//					@Override
//					public String map(Tuple2<String, Integer> tuple)
//							throws Exception {
//						return tuple.f0 + "\t" + tuple.f1;
//					}
//				});
//
//		dataStream.addSink(new FlinkKafkaProducer<String>(PropertiesStack
//				.getResultKafkaTopic(),
//				new SerializationSchema<String, byte[]>() {
//					@Override
//					public byte[] serialize(String element) {
//						return element.getBytes();
//					}
//				}, producerProperties));
//
//		env.execute("Window WordCount");
//	}
//
//	public static void main(String[] args) {
//		try {
//			System.out.println("program started at "
//					+ System.currentTimeMillis());
//			new Kafka82DefaultStreamWordCount().run();
//		} catch (TimeoutException e) {
//			System.out.println(e.getMessage());
//			System.out
//					.println("program ended at " + System.currentTimeMillis());
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//}

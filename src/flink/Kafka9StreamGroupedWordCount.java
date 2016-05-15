package flink;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationKeyValueSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import utils.PropertiesStack;

public class Kafka9StreamGroupedWordCount implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = Logger.getLogger(Kafka9StreamGroupedWordCount.class);
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

		DataStream<Tuple2<String, String>> dataStream = getKeyedWordCount(env,
				consumerProperties);
		// dataStream.print();
		dataStream.addSink(new FlinkKafkaProducer09<>(PropertiesStack
				.getResultKafkaTopic(),
				new KeyedSerializationSchema<Tuple2<String, String>>() {

					@Override
					public byte[] serializeKey(Tuple2<String, String> element) {
						return element.f0.getBytes();
					}

					@Override
					public byte[] serializeValue(Tuple2<String, String> element) {
						return element.f1.getBytes();
					}
				}, producerProperties));

		env.execute("Window WordCount");
	}

	private DataStream<Tuple2<String, String>> getKeyedWordCount(
			StreamExecutionEnvironment env, Properties consumerProperties) {
		return env
				// .readFileStream(filePath, intervalMillis, watchType)
				.addSource(
						new FlinkKafkaConsumer09<Tuple2<String, String>>(
								PropertiesStack.getKafkaTopic(),
								new KafkaKeyedDeserializationSchema(),
								consumerProperties))
				.flatMap(new KeyedLineSplitter())
				.keyBy(0)
				.timeWindow(
						Time.of(PropertiesStack.getFlinkWindowPeriod(),
								TimeUnit.SECONDS))
				// .trigger(EventTimeTrigger.create())
//				.trigger(
//						ContinuousEventTimeTrigger.of(Time.of(
//								PropertiesStack.getFlinkWindowPeriod(),
//								TimeUnit.SECONDS)))
				.sum(1)
				.map(new MapFunction<Tuple2<Tuple2<String, String>, Integer>, Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> map(
							Tuple2<Tuple2<String, String>, Integer> value)
							throws Exception {
						return new Tuple2<>(value.f0.f0, value.f0.f1 + "\t"
								+ value.f1);
					}
				});
	}

	// private DataStream<Tuple2<String, String>> getKeyedWordCount(
	// StreamExecutionEnvironment env, Properties consumerProperties) {
	// return env
	// // .readFileStream(filePath, intervalMillis, watchType)
	// .addSource(
	// new FlinkKafkaConsumer09<Tuple2<String, String>>(
	// PropertiesStack.getKafkaTopic(),
	// new KafkaKeyedDeserializationSchema(),
	// consumerProperties))
	// .keyBy(0)
	// .timeWindow(
	// Time.of(PropertiesStack.getFlinkWindowPeriod(),
	// TimeUnit.SECONDS))
	// //.trigger(EventTimeTrigger.create())
	// // .trigger(
	// // ContinuousEventTimeTrigger.of(Time.of(
	// // PropertiesStack.getFlinkWindowPeriod(),
	// // TimeUnit.SECONDS)))
	// .reduce(new ReduceFunction<Tuple2<String, String>>() {
	//
	// @Override
	// public Tuple2<String, String> reduce(
	// Tuple2<String, String> value1,
	// Tuple2<String, String> value2) throws Exception {
	// return new Tuple2<String, String>(value1.f0, value1.f1
	// + " " + value2.f1);
	// }
	// }).flatMap(new GroupedLinesFlatMapFunction());
	// }

	public static void main(String[] args) {
		try {
			System.out.println("program started at "
					+ System.currentTimeMillis());
			new Kafka9StreamGroupedWordCount().run();
		} catch (TimeoutException e) {
			System.out.println(e.getMessage());
			System.out
					.println("program ended at " + System.currentTimeMillis());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

package flink;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileMonitoringFunction.WatchType;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import utils.PropertiesStack;

public class HDFSStreamWordCount implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static Logger logger = Logger.getLogger(HDFSStreamWordCount.class);
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
		DataStream<String> dataStream = env
				.readFileStream(PropertiesStack.getHdfsSource(), 600000,
						WatchType.ONLY_NEW_FILES).flatMap(new LineSplitter())
				.keyBy(0).timeWindow(Time.of(10, TimeUnit.SECONDS)).sum(1)
				.map(new MapFunction<Tuple2<String, Integer>, String>() {
					@Override
					public String map(Tuple2<String, Integer> tuple)
							throws Exception {
						return tuple.f0 + "\t" + tuple.f1;
					}
				});

		dataStream.addSink(new RollingSink<>(PropertiesStack.getHdfsSink()));

		env.execute("Window WordCount");
	}

	public static void main(String[] args) {
		try {
			System.out.println("program started at "
					+ System.currentTimeMillis());
			new HDFSStreamWordCount().run();
		} catch (TimeoutException e) {
			System.out.println(e.getMessage());
			System.out
					.println("program ended at " + System.currentTimeMillis());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

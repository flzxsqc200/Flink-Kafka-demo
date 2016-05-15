package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class KeyedLineSplitter
		implements
		FlatMapFunction<Tuple2<String, String>, Tuple2<Tuple2<String, String>, Integer>> {
	private static final long serialVersionUID = -6727406628637950869L;

	@Override
	public void flatMap(Tuple2<String, String> value,
			Collector<Tuple2<Tuple2<String, String>, Integer>> out) {
		String[] tokens = value.f1.toLowerCase().split("\\W+");
		for (String token : tokens) {
			if (token.length() > 0) {
				out.collect(new Tuple2<Tuple2<String, String>, Integer>(
						new Tuple2<String, String>(value.f0, token), 1));
			}
		}
	}
}

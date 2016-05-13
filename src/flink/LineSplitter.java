package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class LineSplitter implements
		FlatMapFunction<String, Tuple2<String, Integer>> {

	// private static long count = 0;
	// private final static long NUM_OF_RECORDS = 1500000;
	// private final static long NUM_OF_RECORDS = 15;
	/**
	 * 
	 */
	private static final long serialVersionUID = -6727406628637950869L;

	@Override
	public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
		// normalize and split the line into words
		//System.out.println(value);
		String[] tokens = value.toLowerCase().split("\\W+");
		// count++;
		// if (count <= NUM_OF_RECORDS) {
		// emit the pairs
		for (String token : tokens) {
			if (token.length() > 0) {
				out.collect(new Tuple2<String, Integer>(token, 1));
			}
		}
		// } else {
		// System.out.println(NUM_OF_RECORDS + " have been processed");
		// System.out.println("Time is " + new Date().getTime());
		// }
	}
}

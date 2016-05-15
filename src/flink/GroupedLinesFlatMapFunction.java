package flink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class GroupedLinesFlatMapFunction implements
		FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8674728627676489666L;

	@Override
	public void flatMap(Tuple2<String, String> value,
			Collector<Tuple2<String, String>> words) throws Exception {

		List<String> tmpWords = Arrays.asList(value.f1.toLowerCase().split(
				"\\W+"));
		Map<String, Integer> wordCountMap = new HashMap<>();

		for (String word : tmpWords) {
			if (wordCountMap.containsKey(word)) {
				wordCountMap.put(word, wordCountMap.get(word) + 1);
			} else {
				wordCountMap.put(word, 1);
			}
		}

		Set<String> groupWords = wordCountMap.keySet();
		for (String word : groupWords) {
			words.collect(new Tuple2<String, String>(value.f0, word + "\t"
					+ wordCountMap.get(word)));
		}

		// return words;
	}

}

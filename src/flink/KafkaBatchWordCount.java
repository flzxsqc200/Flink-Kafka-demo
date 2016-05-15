package flink;

import java.util.Arrays;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.mapreduce.Job;

public class KafkaBatchWordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		// get input data
		DataSet<String> text = getTextDataSet(env);

		DataSet<Tuple2<String, Integer>> counts =
		// normalize and split each line
		text.map(line -> line.toLowerCase().split("\\W+"))
				// convert splitted line in pairs (2-tuples) containing:
				// (word,1)
				.flatMap(
						(String[] tokens, Collector<Tuple2<String, Integer>> out) -> {
							// emit the pairs with non-zero-length words
							Arrays.stream(tokens)
									.filter(t -> t.length() > 0)
									.forEach(
											t -> out.collect(new Tuple2<>(t, 1)));
						})
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0).sum(1);

		// emit result
		if (fileOutput) {
			counts.writeAsCsv(outputPath, "\n", " ");
		} else {
			counts.print();
		}

		// execute program
		env.execute("WordCount Example");
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = true;
	private static String textPath="/home/fedora/tweets_small";
	private static String outputPath="/home/fedora/tweets_small_flink_output";

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err
						.println("Usage: WordCount <text path> <result path>");
				return false;
			}
		} else {
			System.out
					.println("Executing WordCount example with built-in default data.");
			System.out
					.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WordCount <text path> <result path>");
		}
		return true;
	}

	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
			return env.readTextFile(textPath);
	}
}

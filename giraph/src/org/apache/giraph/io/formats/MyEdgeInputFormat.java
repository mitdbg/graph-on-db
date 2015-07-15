package org.apache.giraph.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.utils.LongPair;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class MyEdgeInputFormat extends TextEdgeInputFormat<LongWritable, FloatWritable> {
	/** Splitter for endpoints */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	public EdgeReader<LongWritable, FloatWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongFloatTextEdgeReader();
	}

	/**
	 * {@link org.apache.giraph.io.EdgeReader} associated with
	 * {@link LongFloatTextEdgeInputFormat}.
	 */
	public class LongFloatTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<LongPair> {
		protected LongPair preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new LongPair(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]));
		}

		protected LongWritable getSourceVertexId(LongPair endpoints) throws IOException {
			return new LongWritable(endpoints.getFirst());
		}

		protected LongWritable getTargetVertexId(LongPair endpoints) throws IOException {
			return new LongWritable(endpoints.getSecond());
		}

		protected FloatWritable getValue(LongPair endpoints) throws IOException {
			//return NullWritable.get();
			return new FloatWritable(1);
		}
	}
}

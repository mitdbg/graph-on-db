package org.apache.giraph.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.utils.LongDoublePair;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexValueInputFormat}
 * for long ids and double values.
 *
 * Each line consists of: id, value
 *
 * @param <E> Edge value
 * @param <M> Message data
 */
public class MyVertexValueInputFormat<E extends Writable, M extends Writable> extends TextVertexValueInputFormat<LongWritable, DoubleWritable, E, M> {
	/** Separator for id and value */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongDoubleTextVertexValueReader();
	}

	/**
	 * {@link org.apache.giraph.io.VertexValueReader} associated with
	 * {@link LongDoubleTextVertexValueInputFormat}.
	 */
	public class LongDoubleTextVertexValueReader extends TextVertexValueReaderFromEachLineProcessed<LongDoublePair> {
		protected LongDoublePair preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new LongDoublePair(Long.valueOf(tokens[0]), Double.valueOf(tokens[1]));
		}

		protected LongWritable getId(LongDoublePair data) throws IOException {
			return new LongWritable(data.getFirst());
		}

		protected DoubleWritable getValue(LongDoublePair data) throws IOException {
			return new DoubleWritable(data.getSecond());
		}
	}
}

package org.apache.giraph.io.formats.cc;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.giraph.utils.LongPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CCVertexValueInputFormat<E extends Writable, M extends Writable> extends TextVertexValueInputFormat<LongWritable, LongWritable, E, M> {
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongLongTextVertexValueReader();
	}
	public class LongLongTextVertexValueReader extends TextVertexValueReaderFromEachLineProcessed<LongPair> {
		protected LongPair preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new LongPair(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]));
		}
		protected LongWritable getId(LongPair data) throws IOException {
			return new LongWritable(data.getFirst());
		}
		protected LongWritable getValue(LongPair data) throws IOException {
			return new LongWritable(data.getSecond());
		}
	}
}
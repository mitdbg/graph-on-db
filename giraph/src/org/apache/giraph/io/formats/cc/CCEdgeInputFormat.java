package org.apache.giraph.io.formats.cc;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.LongPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CCEdgeInputFormat extends TextEdgeInputFormat<LongWritable, NullWritable> {
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongNullTextEdgeReader();
	}
	public class LongNullTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<LongPair> {
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
		protected NullWritable getValue(LongPair endpoints) throws IOException {
			return NullWritable.get();
		}
	}
}
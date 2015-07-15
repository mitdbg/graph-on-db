package org.apache.giraph.io.formats;


import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;

import org.apache.giraph.io.EdgeReader;

import core.advanced.SGDWithCollabFiltering;
import core.utils.IntDoubleSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class CustomIntDoubleTextEdgeInputFormat extends TextEdgeInputFormat<IntWritable, DoubleWritable> {
	/** Splitter for endpoints */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	private static final Logger LOG = Logger.getLogger(CustomIntDoubleTextEdgeInputFormat.class);
	public EdgeReader<IntWritable, DoubleWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new IntDoubleTextEdgeReader();
	}

	/**
	 * {@link org.apache.giraph.io.EdgeReader} associated with
	 * {@link LongFloatTextEdgeInputFormat}.
	 */
	public class IntDoubleTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<IntDoubleSet> {
		// use any object/java class (e.g. longtriple)
		protected IntDoubleSet preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			return new IntDoubleSet(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Double.valueOf(tokens[2]));
		}

		protected IntWritable getSourceVertexId(IntDoubleSet edgeData) throws IOException {
			return new IntWritable(edgeData.getFirst());
		}

		protected IntWritable getTargetVertexId(IntDoubleSet edgeData) throws IOException {
			return new IntWritable(edgeData.getSecond());
		}

		protected DoubleWritable getValue(IntDoubleSet edgeData) throws IOException {
			//return NullWritable.get();
			return new DoubleWritable(edgeData.getThird());
		}

		/*protected Text getVertexType(IntDoubleSet edgeData) throws IOException {
			char type = String.valueOf(edgeData.getFirst()).charAt(0);
			return new Text(String.valueOf(type));

		} */
	}
}

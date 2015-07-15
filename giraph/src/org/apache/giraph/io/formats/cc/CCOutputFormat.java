package org.apache.giraph.io.formats.cc;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CCOutputFormat extends TextVertexOutputFormat<LongWritable, LongWritable, NullWritable> {
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new CCVertexWriter();
	}
	public class CCVertexWriter extends TextVertexWriter {
		public void writeVertex(Vertex<LongWritable, LongWritable, NullWritable, ?> vertex) throws IOException, InterruptedException {
			getRecordWriter().write(
					new Text(vertex.getId().toString()),
					new Text(vertex.getValue().toString()));
		}
	}
}
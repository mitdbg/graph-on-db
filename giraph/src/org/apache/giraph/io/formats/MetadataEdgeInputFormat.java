package org.apache.giraph.io.formats;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.utils.Metadata;
import core.utils.Metadata.Type;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class MetadataEdgeInputFormat extends TextEdgeInputFormat<LongWritable, FloatWritable> {
	public EdgeReader<LongWritable, FloatWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new MetadataEdgeReader(getTypes());
	}
	protected Type[] getTypes(){
		return new Type[]{Type.LONG, Type.LONG, Type.STRING, Type.INT, Type.STRING};
	}
	
	public class MetadataEdgeReader extends TextEdgeReader{
		protected Metadata edgeData;
		protected Type[] types;
		public MetadataEdgeReader(Type[] types){
			this.types = types;
		}
		public Edge<LongWritable, FloatWritable> getCurrentEdge() throws IOException, InterruptedException {
			return EdgeFactory.create(
							new LongWritable(edgeData.getLong(1)),	// target vertex id 
							new FloatWritable(1)					// edge value			
							);
		}
		public LongWritable getCurrentSourceId() throws IOException, InterruptedException {
			return new LongWritable(edgeData.getLong(0));			// source vertex id
		}
		public boolean nextEdge() throws IOException, InterruptedException {
			if(getRecordReader().nextKeyValue()){
				parseEdge(getRecordReader().getCurrentValue());
				return true;
			}
			else
				return false;
		}
		protected void parseEdge(Text line){
			edgeData = new Metadata(line.toString(), types);
		}		
	}
}

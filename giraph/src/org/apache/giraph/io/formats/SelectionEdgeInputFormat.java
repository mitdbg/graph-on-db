package org.apache.giraph.io.formats;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.utils.Metadata.Type;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with int ids.
 *
 * Each line consists of: source_vertex, target_vertex
 */
public class SelectionEdgeInputFormat extends MetadataEdgeInputFormat {
	public EdgeReader<LongWritable, FloatWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new SelectionEdgeReader(getTypes());
	}
	
	public class SelectionEdgeReader extends MetadataEdgeReader{
		public SelectionEdgeReader(Type[] types){
			super(types);
		}
		public boolean nextEdge() throws IOException, InterruptedException {
			while(getRecordReader().nextKeyValue()){
				parseEdge(getRecordReader().getCurrentValue());
				if(edgeData.getString(2).equals("Family"))		// get only the edges of type "Family"
					return true;
			}
			return false;
		}
	}
}

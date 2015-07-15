package org.apache.giraph.io.formats;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.utils.Metadata.Type;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexValueInputFormat}
 * for long ids and double values.
 *
 * Each line consists of: id, value
 *
 * @param <E> Edge value
 * @param <M> Message data
 */
public class SelectionVertexValueInputFormat<E extends Writable, M extends Writable> extends MetadataVertexValueInputFormat<E, M> {
	public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new SelectionVertexValueReader(getTypes());
	}

	public class SelectionVertexValueReader extends MetadataVertexValueReader{
		public SelectionVertexValueReader(Type[] types){
			super(types);
		}
		public boolean nextVertex() throws IOException, InterruptedException {
			while(getRecordReader().nextKeyValue()){
				parseVertex(getRecordReader().getCurrentValue());
				if(vertexData.getInteger(7).equals(4))		// get only the edges of type "a6=4"
					return true;
			}
			return false;
		}
	}
}

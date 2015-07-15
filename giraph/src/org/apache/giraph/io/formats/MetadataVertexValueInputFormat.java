package org.apache.giraph.io.formats;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import core.utils.Metadata;
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
public class MetadataVertexValueInputFormat<E extends Writable, M extends Writable> extends TextVertexValueInputFormat<LongWritable, DoubleWritable, E, M> {
	public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new MetadataVertexValueReader(getTypes());
	}
	protected Type[] getTypes(){
		Type[] types = new Type[62];
		types[0] = Type.LONG;
		types[1] = Type.DOUBLE;
		for(int i=2;i<34;i++)
			types[i] = Type.INT;
		for(int i=34;i<52;i++)
			types[i] = Type.FLOAT;
		for(int i=52;i<62;i++)
			types[i] = Type.STRING;
		return types;
	}

	public class MetadataVertexValueReader extends TextVertexValueReader{
		protected Metadata vertexData;
		protected Type[] types;
		public MetadataVertexValueReader(Type[] types){
			this.types = types;
		}
		public LongWritable getCurrentVertexId() throws IOException, InterruptedException {
			return new LongWritable(vertexData.getLong(0));
		}
		public DoubleWritable getCurrentVertexValue() throws IOException, InterruptedException {
			return new DoubleWritable(vertexData.getDouble(1));
		}
		public boolean nextVertex() throws IOException, InterruptedException {
			if(getRecordReader().nextKeyValue()){
				parseVertex(getRecordReader().getCurrentValue());
				return true;
			}
			else
				return false;
		}
		protected void parseVertex(Text line){
			vertexData = new Metadata(line.toString(), types);
		}
	}
}

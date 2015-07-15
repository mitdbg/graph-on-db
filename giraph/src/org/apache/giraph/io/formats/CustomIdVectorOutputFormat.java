package org.apache.giraph.io.formats;


import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Write out Vertices' IDs and values, but not their edges nor edges' values.
 * This is a useful output format when the final value of the vertex is
 * all that's needed. The boolean configuration parameter reverse.id.and.value
 * allows reversing the output of id and value.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class CustomIdVectorOutputFormat extends TextVertexOutputFormat<IntWritable, Text, DoubleWritable> {

  /** Specify the output delimiter */
  public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
  /** Default output delimiter */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
  
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
    return new IdWithVectorVertexWriter();
  }

  /**
   * Vertex writer used with {@link IdWithValueTextOutputFormat}.
   */
  protected class IdWithVectorVertexWriter extends TextVertexWriterToEachLine {
    /** Saved delimiter */
    private String delimiter;
    /** Cached reserve option */
    private boolean reverseOutput;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
        InterruptedException {
      super.initialize(context);
      delimiter = getConf().get(
          LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    @Override
    protected Text convertVertexToLine(Vertex<IntWritable, Text, DoubleWritable, ?> vertex)
      throws IOException {
      String vertexInfo = vertex.getId().toString();
      String latentVector = vertex.getValue().toString();
      vertexInfo += delimiter + latentVector;
      return new Text(vertexInfo);
    }

  }

}
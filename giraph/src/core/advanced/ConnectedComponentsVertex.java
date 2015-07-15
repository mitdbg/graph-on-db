/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package core.advanced;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id
 * in the component)
 *
 * The idea behind the algorithm is very simple: propagate the smallest
 * vertex id along the edges to all vertices of a connected component. The
 * number of supersteps necessary is equal to the length of the maximum
 * diameter of all components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 */
public class ConnectedComponentsVertex extends Vertex<LongWritable,
    LongWritable, NullWritable, LongWritable> {
  /**
   * Propagates the smallest vertex id to all neighbors. Will always choose to
   * halt and only reactivate if a smaller id has been sent to it.
   *
   * @param messages Iterator of messages from the previous superstep.
   * @throws IOException
   */
  @Override
  public void compute(Iterable<LongWritable> messages) throws IOException {
    long currentComponent = getValue().get();

    // First superstep is special, because we can simply look at the neighbors
    if (getSuperstep() == 0) {
      for (Edge<LongWritable, NullWritable> edge : getEdges()) {
        long neighbor = edge.getTargetVertexId().get();
        if (neighbor < currentComponent) {
          currentComponent = neighbor;
        }
      }
      // Only need to send value if it is not the own id
      if (currentComponent != getValue().get()) {
        setValue(new LongWritable(currentComponent));
        for (Edge<LongWritable, NullWritable> edge : getEdges()) {
          LongWritable neighbor = edge.getTargetVertexId();
          if (neighbor.get() > currentComponent) {
            sendMessage(neighbor, getValue());
          }
        }
      }

      voteToHalt();
      return;
    }

    boolean changed = false;
    // did we get a smaller id ?
    for (LongWritable message : messages) {
      long candidateComponent = message.get();
      if (candidateComponent < currentComponent) {
        currentComponent = candidateComponent;
        changed = true;
      }
    }

    // propagate new component id to the neighbors
    if (changed) {
      setValue(new LongWritable(currentComponent));
      sendMessageToAllEdges(getValue());
    }
    voteToHalt();
  }
  
  
  
  
  
  
  
//  public static class CCVertexValueInputFormat<E extends Writable, M extends Writable> extends TextVertexValueInputFormat<LongWritable, LongWritable, E, M> {
//		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
//		public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
//			return new LongLongTextVertexValueReader();
//		}
//		public class LongLongTextVertexValueReader extends TextVertexValueReaderFromEachLineProcessed<LongPair> {
//			protected LongPair preprocessLine(Text line) throws IOException {
//				String[] tokens = SEPARATOR.split(line.toString());
//				return new LongPair(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]));
//			}
//			protected LongWritable getId(LongPair data) throws IOException {
//				return new LongWritable(data.getFirst());
//			}
//			protected LongWritable getValue(LongPair data) throws IOException {
//				return new LongWritable(data.getSecond());
//			}
//		}
//	}
//  
//  public static class CCEdgeInputFormat extends TextEdgeInputFormat<LongWritable, NullWritable> {
//		private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
//		public EdgeReader<LongWritable, NullWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
//			return new LongNullTextEdgeReader();
//		}
//		public class LongNullTextEdgeReader extends TextEdgeReaderFromEachLineProcessed<LongPair> {
//			protected LongPair preprocessLine(Text line) throws IOException {
//				String[] tokens = SEPARATOR.split(line.toString());
//				return new LongPair(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]));
//			}
//			protected LongWritable getSourceVertexId(LongPair endpoints) throws IOException {
//				return new LongWritable(endpoints.getFirst());
//			}
//			protected LongWritable getTargetVertexId(LongPair endpoints) throws IOException {
//				return new LongWritable(endpoints.getSecond());
//			}
//			protected NullWritable getValue(LongPair endpoints) throws IOException {
//				return NullWritable.get();
//			}
//		}
//	}
}

package core.advanced;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

@Algorithm(
    name = "Triangle Counting",
    description = "Counts the number of triangle in the entire graph"
)
public class TriangleCountingVertex extends Vertex<LongWritable, DoubleWritable, FloatWritable, Text>{

	private static final Logger LOG = Logger.getLogger(TriangleCountingVertex.class);
	private static String TRIANGLE_AGG = "triangles";
	
	protected Multimap<Long, Long> oneHopNeighbors;
	
	protected List<Long> sortNeighbors(){
		List<Long> neighbors = new ArrayList<Long>();
		for (Edge<LongWritable, FloatWritable> edge : getEdges())
			neighbors.add(edge.getTargetVertexId().get());
		Collections.sort(neighbors);
		return neighbors;
	}
	
	protected void sendNeighbors(){
		List<Long> neighbors = sortNeighbors();
		long id = getId().get();
		String neighborStr = "";
		for(long n: neighbors){
			if(n > id && !neighborStr.equals(""))
				sendMessage(new LongWritable(n), new Text(id+":"+neighborStr));
			neighborStr += n+",";
		}
	}
	
	protected Multimap<Long, Long> parseMessages(Iterable<Text> messages){
		for(Text m: messages){
			String[] tokens = m.toString().split(":");
			long key = Long.parseLong(tokens[0]);
			for(String v: tokens[1].split(","))
				if(!v.equals(""))
					oneHopNeighbors.put(key, Long.parseLong(v));
		}
		return oneHopNeighbors;
	}
	
	@Override
	public void compute(Iterable<Text> arg0) throws IOException {
		if(getSuperstep() == 0)
			sendNeighbors();		// first super step
		else{
			// second super step
			oneHopNeighbors = ArrayListMultimap.create();
			parseMessages(arg0);
			long triangles = 0;
			for(Long key: oneHopNeighbors.keySet())
				for(Long n: oneHopNeighbors.get(key))
					if(key < n && oneHopNeighbors.containsKey(n))
						triangles++;
			setValue(new DoubleWritable(triangles));
			//aggregate(TRIANGLE_AGG, new LongWritable(triangles));
			voteToHalt();
		}
	}
	
	public static class TriangleCountingVertexMasterCompute extends DefaultMasterCompute {
		public void initialize() throws InstantiationException, IllegalAccessException {
		    registerPersistentAggregator(TRIANGLE_AGG, LongSumAggregator.class);
		}
	}
	
	public static class TriangleCountingVertexWorkerContext extends WorkerContext {
		public void postApplication() {
			long FINAL_SUM = this.<LongWritable>getAggregatedValue(TRIANGLE_AGG).get();
			LOG.info("aggregatedNumTriangles=" + FINAL_SUM);
			System.out.println("NUmber of Triangles = "+FINAL_SUM);
		}
		public void postSuperstep() {
		}
		public void preApplication() throws InstantiationException, IllegalAccessException {
		}
		public void preSuperstep() {
		}
	}
}

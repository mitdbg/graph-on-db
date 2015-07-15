package core.advanced;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

@Algorithm(
    name = "Maximal Cliques",
    description = "Counts the number of maximal cliques per node in the entire graph"
)
public class MaximalCliquesVertex extends WeakTiesVertex{

	private int numOfCliques;
	
	protected void sendNeighbors(){
		List<Long> neighbors = sortNeighbors();
		long id = getId().get();
		String neighborStr = Joiner.on(",").join(getNeighborIds());
		for(long n: neighbors)
			if(n > id && !neighborStr.equals(""))
				sendMessage(new LongWritable(n), new Text(id+":"+neighborStr));
	}
	
	private Multimap<Long,Long> createSubGraph(Multimap<Long, Long> neighbors){
		long id = getId().get();
		for (Edge<LongWritable, FloatWritable> edge : getEdges())
			neighbors.put(edge.getTargetVertexId().get(), id);
		return neighbors;
	}
	
	private long getU(Multimap<Long,Long> G_k_sub, Set<Long> S_sub, Set<Long> S_can){
		int minIntersect = Integer.MIN_VALUE;
		long chosenU = -1;
		for(Long u: S_sub){
			int intersect = Sets.intersection(S_can, (Set<Long>)(G_k_sub.get(u))).size();
			if(intersect > minIntersect){
				minIntersect = intersect;
				chosenU = u;
			}
		}
		return chosenU;
	}

	private void enumerate(
			long k,
			Multimap<Long,Long> G_k_sub, 
			Set<Long> S_sub, 
			Set<Long> S_can, 
			Set<Long> C
			)
	{
		if(S_sub.size()==0){
			if(C.size()>=3){
				numOfCliques++;		// clique detected!
			}
		}
		else{
			Long u = getU(G_k_sub, S_sub, S_can);
			Set<Long> S_ext = Sets.difference(S_can, (Set<Long>)(G_k_sub.get(u)));
			while(G_k_sub.get(u)!=null && S_ext.size()!=0){
				long q = Collections.max(S_ext);
				if(q > k){
					S_can.remove(q);
					S_ext.remove(q);
					continue;
				}
				else{
					C.add(q);
					Set<Long> S_q_sub = Sets.intersection(S_sub, (Set<Long>)(G_k_sub.get(q)));
					Set<Long> S_q_can = Sets.intersection(S_can, (Set<Long>)(G_k_sub.get(q)));
					enumerate(k, G_k_sub, Sets.newHashSet(S_q_sub), Sets.newHashSet(S_q_can), C);
					S_can.remove(q);
					S_ext.remove(q);
					C.remove(q);
				}
			}
		}
	}


	@Override
	public void compute(Iterable<Text> arg0) throws IOException {
		if(getSuperstep() == 0)
			sendNeighbors();		// first super step
		else{
			numOfCliques = 0;
			long k = getId().get();
			oneHopNeighbors = HashMultimap.create();
			Multimap<Long,Long> G_k_sub = createSubGraph(parseMessages(arg0));
			enumerate(k, G_k_sub, Sets.newHashSet(G_k_sub.keySet()), Sets.newHashSet(G_k_sub.keySet()), Sets.newHashSet(k));
			setValue(new DoubleWritable(numOfCliques));
			voteToHalt();
		}
	}
}

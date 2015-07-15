package core.advanced;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Algorithm(
    name = "Weak Ties",
    description = "Enumerate vertices which serve as conduit for sufficient number of weak ties."
)
public class WeakTiesVertex extends TriangleCountingVertex{

	public static final LongConfOption THRESHOLD = new LongConfOption("VertexValue.threshold", 1);
	protected boolean checkThreshold(long weakTies) {
		return weakTies >= THRESHOLD.get(getConf());
	}
	
//	protected Multimap<Long, Long> oneHopNeighbors;
	
	protected Set<Long> getNeighborIds(){
		Set<Long> neighbors = Sets.newHashSet();
		for (Edge<LongWritable, FloatWritable> edge : getEdges())
			neighbors.add(edge.getTargetVertexId().get());
		return neighbors;
	}
	
//	protected Multimap<Long, Long> parseMessages(Iterable<BytesWritable> messages){
//		for(BytesWritable m: messages){
//			byte[] bytes = BinaryUtils.resize(m.getBytes(), m.getLength());
//			List<Long> ids = BinaryUtils.getLongArray(CompressionUtils.decompressGZIP(bytes));
//			long key = ids.get(0);
//			for(int i=1;i<ids.size();i++)
//				oneHopNeighbors.put(key, ids.get(i));
//		}
//		//System.gc();
//		return oneHopNeighbors;
//	}
	
	@Override
	public void compute(Iterable<Text> arg0) throws IOException {
		if(getSuperstep() == 0){
			// first super step
			List<Long> neighbors = Lists.newArrayList(getNeighborIds());
			neighbors.add(0, getId().get());
			sendMessageToAllEdges(new Text(
//											CompressionUtils.compressGZIP(BinaryUtils.toBytes(neighbors))
											getId().get() + ":" +
											Joiner.on(",").join(getNeighborIds())
											)
									);
		}
		else if(getSuperstep() == 1){
			// second super step
			oneHopNeighbors = HashMultimap.create();
			parseMessages(arg0);
			Set<Long> neighbors = getNeighborIds();
			long weakTies = 0;
			
			for(long n: neighbors){
				Set<Long> nNeighbors = (Set<Long>)(oneHopNeighbors.get(n));
				weakTies += Sets.difference(neighbors, nNeighbors).size() - 1;
			}
			weakTies /= 2;			
			if(checkThreshold(weakTies))
				setValue(new DoubleWritable(weakTies));
			voteToHalt();	
		}
	}
}

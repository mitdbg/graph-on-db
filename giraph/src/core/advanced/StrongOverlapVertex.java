package core.advanced;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.Algorithm;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

@Algorithm(
    name = "Strong Ties",
    description = "Enumerate vertex-pairs which have sufficiently strong ties (common neighbors)."
)
public class StrongOverlapVertex extends WeakTiesVertex{
	
	protected void sendNeighbors(){
		List<Long> neighbors = sortNeighbors();
		long id = getId().get();
		String neighborStr = "";
		for(long n: neighbors){
			if(!neighborStr.equals(""))
				sendMessage(new LongWritable(n), new Text(id+":"+neighborStr));
			neighborStr += n+",";
		}
	}
	
	@Override
	public void compute(Iterable<Text> arg0) throws IOException {
		if(getSuperstep() == 0)
			sendNeighbors();		// first super step
		else{
			// second super step
			oneHopNeighbors = HashMultimap.create();
			parseMessages(arg0);
			long strongTies = 0;
			
			Multiset<Long> histogram = HashMultiset.create(oneHopNeighbors.values());
			for(Long k: histogram.elementSet())
				if(checkThreshold(histogram.count(k)))
					strongTies++;
			
			setValue(new DoubleWritable(strongTies));
			voteToHalt();
		}
	}
}

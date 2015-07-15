package core.advanced;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;

public class PerNodeTriangleCountingVertex extends TriangleCountingVertex{

	private void addTriangleCount(Map<Long,Long> perNodeTriangles, long nodeId){
		if(perNodeTriangles.containsKey(nodeId))
			perNodeTriangles.put(nodeId,perNodeTriangles.get(nodeId)+1);
		else
			perNodeTriangles.put(nodeId, 1l);
	}
	
	@Override
	public void compute(Iterable<Text> arg0) throws IOException {
		if(getSuperstep() == 0)
			sendNeighbors();		// first super step
		else if(getSuperstep() == 1){
			oneHopNeighbors = ArrayListMultimap.create();
			parseMessages(arg0);
			Map<Long,Long> perNodeTriangles = Maps.newHashMap();
			long triangles = 0;
			for(Long key: oneHopNeighbors.keySet())
				for(Long n: oneHopNeighbors.get(key))
					if(key < n && oneHopNeighbors.containsKey(n)){
						addTriangleCount(perNodeTriangles, key);
						addTriangleCount(perNodeTriangles, n);
						triangles++;
					}
			setValue(new DoubleWritable(triangles));
			for(Long nodeId: perNodeTriangles.keySet())
				sendMessage(new LongWritable(nodeId), new Text(""+perNodeTriangles.get(nodeId)));
			voteToHalt();
		}
		else{
			long triangles = 0;
			for(Text m: arg0)
				triangles += Long.parseLong(m.toString());
			setValue(new DoubleWritable(getValue().get()+triangles));
			voteToHalt();
		}
	}
}

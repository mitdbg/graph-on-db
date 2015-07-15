package core.advanced;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.Algorithm;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@Algorithm(
    name = "Stochastic Gradient Descent",
    description = "Performs collaborative filtering with SGD as the error minimizing function"
)

public class SGDWithCollabFiltering extends Vertex<IntWritable, Text, DoubleWritable, Text>{

	/** Number of supersteps for this test */
	public static final int MAX_SUPERSTEPS = 5;
	/** Sum aggregator for error**/
	private static String SUM_ERROR_AGG = "sum";
	/** Int aggregator for number of edges **/
	/** Logger */
	private static final Logger LOG = Logger.getLogger(SGDWithCollabFiltering.class);
	public static final double lambda = 0.01;
	public static final double gamma = 0.005;
	public static final int vectorSize = 2;
	private double dotProduct(ArrayList<Double> a1, ArrayList<Double> a2) {
		double sum = 0;
		if (a1.size() == a2.size()) {
			int length = a1.size();
			for (int i = 0; i < length; i++) {
				sum += a1.get(i) * a2.get(i);
			}
		}
		return sum;
	}

	private Text prepareMsg(String vertexValue, int id) {
		String msg = String.valueOf(id);
		String[] latentVector = vertexValue.split(",");
		for (int i = 0; i < latentVector.length; i++) {
			msg+=","+latentVector[i];
		}
		return new Text(msg);

	}

	private int getSourceId(Text message) {
		String[] tokens = message.toString().split(",");
		int id = Integer.parseInt(tokens[0]);
		return id;

	}

	private ArrayList<Double> getLatentVector(Text message, int startfrom) {
		String[] tokens = message.toString().split(",");
		ArrayList<Double> latentVector = new ArrayList<Double>();
		for (int i=startfrom; i < tokens.length; i++) {
			latentVector.add(Double.valueOf(tokens[i]));
		}
		return latentVector;
	}
	
	private ArrayList<Double> scaleVector (ArrayList<Double> vector, double factor) {
		ArrayList<Double> scaledVector = new ArrayList<Double>();
		for (int i = 0; i < vector.size(); i++) {
			scaledVector.add(vector.get(i) * factor);
		}
		return scaledVector;
	}
	
	private ArrayList<Double> addVector (ArrayList<Double> vector1, ArrayList<Double> vector2) {
		assert(vector1.size() == vector2.size());
		ArrayList<Double> newVector = new ArrayList<Double>();
		for (int i = 0; i < vector1.size(); i++) {
			newVector.add(vector1.get(i) + vector2.get(i));
		}
		return newVector;
	}
	

	@Override
	public void compute(Iterable<Text> messages) {
	    long startTime = System.nanoTime();
	    double error;
	    String vertexValue = "";
	    int msgsProcessed = 0;
		if (getSuperstep() >= MAX_SUPERSTEPS) {
			voteToHalt();
			long timeTaken = System.nanoTime() - startTime;
			LOG.info("On superstep: " + getSuperstep() + " - time taken by compute function: "+ timeTaken);
			return;
		}
		if (getSuperstep() == 0) {
			// if vertices are users, send to neighbors the latent vector of current vertex along with the vertex id
			// initialize latent vector for both user and item vertices
			
			for (int i=0; i < vectorSize; i++) {
				// TODO: Think of better initialization technique
				vertexValue += String.valueOf(1);//Math.random());
				if (i != vectorSize - 1) { 
					vertexValue+=",";
				}
			}
		}

		else {
			double rmse = 0;
			long startTimeForComputation = System.nanoTime();
			ArrayList<Double> selfLatentVector = getLatentVector(getValue(), 0);
			
			for (Text message : messages) {
				msgsProcessed += 1;
				// Each message contains latent vector and vertex id
				int sourceVertexId = getSourceId(message); 
				ArrayList<Double> msgLatentVector = getLatentVector(message, 1);
		
				double predictedRating = dotProduct(selfLatentVector, msgLatentVector);
				predictedRating = Math.min(5.0, predictedRating);
				predictedRating = Math.max(1.0, predictedRating);
				if(getEdgeValue(new IntWritable(sourceVertexId))==null)
					LOG.info("NullPointer when getting edge value; sourceVertexId="+sourceVertexId+", vertexId"+getId());
				error = predictedRating - getEdgeValue(new IntWritable(sourceVertexId)).get();
				//LOG.info("predicted rating: "+ predictedRating +" , known rating: " + getEdgeValue(new IntWritable(sourceVertexId)).get() +" error: "+ error);
				ArrayList<Double> part1 = scaleVector(selfLatentVector, lambda);
				ArrayList<Double> part2 = scaleVector(msgLatentVector, error);
				ArrayList<Double> adjustment = addVector(part1, part2);
				selfLatentVector = addVector(selfLatentVector, scaleVector(adjustment, -gamma));
				vertexValue = "";
				for (int i = 0; i < vectorSize; i++) {
					vertexValue += selfLatentVector.get(i).toString();
					if (i != (vectorSize - 1)) {
						vertexValue+=",";
					}
				}
				rmse += Math.pow(error, 2);
			}
			long timeTakenForComputation = System.nanoTime() - startTimeForComputation;

		}
		setValue(new Text(vertexValue));
		Text msg = prepareMsg(vertexValue, getId().get());
		int msgsSent = 0;
		sendMessageToAllEdges(msg);
		//rmse = rmse/getNumEdges();
		/*if (rmse <= 1.0) {
			voteToHalt();
		} */
		voteToHalt();
		long timeTaken = System.nanoTime() - startTime;
		LOG.info("Superstep: "+ getSuperstep() +" Number of edges for vertex Id " + getId() + " #edges: " + getNumEdges() +
				" processed " + msgsProcessed);
		LOG.info("On superstep: " + getSuperstep() + " - time taken by compute function in ns: "+ timeTaken);
		
	}
	
	public static class SGDVertexMasterCompute extends
	DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
		IllegalAccessException {
			registerAggregator(SUM_ERROR_AGG, DoubleSumAggregator.class);
		}
		
		@Override
		public void compute() {
			// Ignore the aggregated value for initialization step
			LOG.info("Master Compute>> Superstep:== " + getSuperstep());
			if (getSuperstep() > 1) {
				double sumErrorSquared = this.<DoubleWritable>getAggregatedValue(SUM_ERROR_AGG).get();
				double rmse = Math.sqrt(sumErrorSquared/getTotalNumEdges());
				LOG.info("Superstep # : " + getSuperstep() +" RMSE: " + rmse);
				// If error is same
				if (Math.abs(rmse) < 0.01) {
					LOG.info("may be halt? rmse less than 0.01 "+ rmse+ " superstep " + getSuperstep());
					//haltComputation();
					
				}
			}
		}
	}

	
}


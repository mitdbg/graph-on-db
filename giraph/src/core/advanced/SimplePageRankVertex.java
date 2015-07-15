package core.advanced;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.examples.GeneratedVertexReader;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.GeneratedVertexInputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class SimplePageRankVertex extends Vertex<LongWritable, DoubleWritable, FloatWritable, DoubleWritable>{

	/** Number of supersteps for this test */
	public static final int MAX_SUPERSTEPS = 10;
	/** Logger */
	private static final Logger LOG = Logger.getLogger(SimplePageRankVertex.class);
	/** Sum aggregator name */
	private static String SUM_AGG = "sum";
	/** Min aggregator name */
	private static String MIN_AGG = "min";
	/** Max aggregator name */
	private static String MAX_AGG = "max";

	@Override
	public void compute(Iterable<DoubleWritable> messages) {
		if (getSuperstep() >= 1) {
			double sum = 0;
			for (DoubleWritable message : messages) {
				sum += message.get();
			}
			DoubleWritable vertexValue =
					new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
			setValue(vertexValue);
			aggregate(MAX_AGG, vertexValue);
			aggregate(MIN_AGG, vertexValue);
			aggregate(SUM_AGG, new LongWritable(1));
			LOG.info(getId() + ": PageRank=" + vertexValue +
					" max=" + getAggregatedValue(MAX_AGG) +
					" min=" + getAggregatedValue(MIN_AGG));
		}

		if (getSuperstep() < MAX_SUPERSTEPS) {
			long edges = getNumEdges();
			sendMessageToAllEdges(
					new DoubleWritable(getValue().get() / edges));
		} else {
			voteToHalt();
		}
	}

	/**
	 * Worker context used with {@link SimplePageRankVertex}.
	 */
	public static class SimplePageRankVertexWorkerContext extends
	WorkerContext {
		/** Final max value for verification for local jobs */
		private static double FINAL_MAX;
		/** Final min value for verification for local jobs */
		private static double FINAL_MIN;
		/** Final sum value for verification for local jobs */
		private static long FINAL_SUM;

		public static double getFinalMax() {
			return FINAL_MAX;
		}

		public static double getFinalMin() {
			return FINAL_MIN;
		}

		public static long getFinalSum() {
			return FINAL_SUM;
		}

		@Override
		public void preApplication()
				throws InstantiationException, IllegalAccessException {
		}

		@Override
		public void postApplication() {
			FINAL_SUM = this.<LongWritable>getAggregatedValue(SUM_AGG).get();
			FINAL_MAX = this.<DoubleWritable>getAggregatedValue(MAX_AGG).get();
			FINAL_MIN = this.<DoubleWritable>getAggregatedValue(MIN_AGG).get();

			LOG.info("aggregatedNumVertices=" + FINAL_SUM);
			LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
			LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
		}

		@Override
		public void preSuperstep() {
			if (getSuperstep() >= 3) {
				LOG.info("aggregatedNumVertices=" +
						getAggregatedValue(SUM_AGG) +
						" NumVertices=" + getTotalNumVertices());
				if (this.<LongWritable>getAggregatedValue(SUM_AGG).get() !=
						getTotalNumVertices()) {
					throw new RuntimeException("wrong value of SumAggreg: " +
							getAggregatedValue(SUM_AGG) + ", should be: " +
							getTotalNumVertices());
				}
				DoubleWritable maxPagerank = getAggregatedValue(MAX_AGG);
				LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
				DoubleWritable minPagerank = getAggregatedValue(MIN_AGG);
				LOG.info("aggregatedMinPageRank=" + minPagerank.get());
			}
		}

		@Override
		public void postSuperstep() { }
	}

	/**
	 * Master compute associated with {@link SimplePageRankVertex}.
	 * It registers required aggregators.
	 */
	public static class SimplePageRankVertexMasterCompute extends
	DefaultMasterCompute {
		@Override
		public void initialize() throws InstantiationException,
		IllegalAccessException {
			registerAggregator(SUM_AGG, LongSumAggregator.class);
			registerPersistentAggregator(MIN_AGG, DoubleMinAggregator.class);
			registerPersistentAggregator(MAX_AGG, DoubleMaxAggregator.class);
		}
	}

	/**
	 * Simple VertexReader that supports {@link SimplePageRankVertex}
	 */
	public static class SimplePageRankVertexReader extends GeneratedVertexReader<LongWritable, DoubleWritable, FloatWritable> {
		/** Class logger */
		private static final Logger LOG = Logger.getLogger(SimplePageRankVertexReader.class);

		@Override
		public boolean nextVertex() {
			return totalRecords > recordsRead;
		}

		@Override
		public Vertex<LongWritable, DoubleWritable, FloatWritable, Writable> getCurrentVertex() throws IOException {
			Vertex<LongWritable, DoubleWritable, FloatWritable, Writable>
			vertex = getConf().createVertex();
			LongWritable vertexId = new LongWritable((inputSplit.getSplitIndex() * totalRecords) + recordsRead);
			DoubleWritable vertexValue = new DoubleWritable(vertexId.get() * 10d);
			long targetVertexId = (vertexId.get() + 1) % (inputSplit.getNumSplits() * totalRecords);
			float edgeValue = vertexId.get() * 100f;
			List<Edge<LongWritable, FloatWritable>> edges = Lists.newLinkedList();
			edges.add(EdgeFactory.create(new LongWritable(targetVertexId), new FloatWritable(edgeValue)));
			vertex.initialize(vertexId, vertexValue, edges);
			++recordsRead;
			if (LOG.isInfoEnabled()) {
				LOG.info("next: Return vertexId=" + vertex.getId().get() +
						", vertexValue=" + vertex.getValue() +
						", targetVertexId=" + targetVertexId + ", edgeValue=" + edgeValue);
			}
			return vertex;
		}
	}

	/**
	 * Simple VertexInputFormat that supports {@link SimplePageRankVertex}
	 */
	public static class SimplePageRankVertexInputFormat extends
	GeneratedVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {
		@Override
		public VertexReader<LongWritable, DoubleWritable,
		FloatWritable> createVertexReader(InputSplit split,TaskAttemptContext context) throws IOException {
			return new SimplePageRankVertexReader();
		}
	}

	/**
	 * Simple VertexOutputFormat that supports {@link SimplePageRankVertex}
	 */
	public static class SimplePageRankVertexOutputFormat extends
	TextVertexOutputFormat<LongWritable, DoubleWritable, FloatWritable> {
		@Override
		public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
			return new SimplePageRankVertexWriter();
		}

		/**
		 * Simple VertexWriter that supports {@link SimplePageRankVertex}
		 */
		public class SimplePageRankVertexWriter extends TextVertexWriter {
			@Override
			public void writeVertex(Vertex<LongWritable, DoubleWritable, FloatWritable, ?> vertex) throws IOException, InterruptedException {
				getRecordWriter().write(
						new Text(vertex.getId().toString()),
						new Text(vertex.getValue().toString()));
			}
		}
	}
}
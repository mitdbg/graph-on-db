package core.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Aggregation {
    public static class AggregationMapTask extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private int valueIdx = 1;
        private int numIntervals;
        private double low, high, interval;
        public void configure(JobConf job) {
        	this.low = Double.parseDouble(job.get("histogramLow"));
        	this.high= Double.parseDouble(job.get("histogramHigh"));
        	this.numIntervals = Integer.parseInt(job.get("histogramNumIntervals"));
        	this.interval = (high-low)/numIntervals;
        }
        public void map(LongWritable key, Text record, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            double val = Double.parseDouble(record.toString().split("\t")[valueIdx]);
            if(val < low){
            	output.collect(new IntWritable(0), new IntWritable(1));
            }
            else{
	            for(int i=1;i<=numIntervals;i++)
	            	if(val>=(low+(i-1)*interval) && val<(low+i*interval)){
	            		output.collect(new IntWritable(i), new IntWritable(1));
	            		return;
	            	}
	            output.collect(new IntWritable(numIntervals), new IntWritable(1));
            }
        }
    }

    public static class AggregationReduceTask extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext())
                sum += values.next().get();
            output.collect(key, new IntWritable(sum));
        }
    }

    public JobConf createJob(String inputFile, String outputFile) throws Exception {
    	JobConf job = new JobConf();
    	job.setJobName(this.getClass().getSimpleName());
    	job.setJarByClass(this.getClass());
    	
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        job.setMapperClass(AggregationMapTask.class);
        job.setReducerClass(AggregationReduceTask.class);
        job.setCombinerClass(AggregationReduceTask.class);
        
        job.setNumReduceTasks(1);
        TextInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
    	
        return job;
    }

    public static void main(String[] args) throws Exception {
    	String inputFile = args[0];
    	String outputFile = args[1];
    	
    	Aggregation agg = new Aggregation();
    	JobConf job = agg.createJob(inputFile, outputFile);
    	
    	job.set("histogramLow", args[2]);
        job.set("histogramHigh", args[3]);
        job.set("histogramNumIntervals", args[4]);
    	
    	JobClient.runJob(job);
	}
}
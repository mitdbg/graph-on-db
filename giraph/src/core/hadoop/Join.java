package core.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Join {
    public static class AggregationMapTask extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        public void map(LongWritable key, Text record, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
        	String[] tokens = record.toString().split("\t");
        	int id = Integer.parseInt(tokens[0]);
        	double value = Double.parseDouble(tokens[1]);        	
        	output.collect(new IntWritable(id), new DoubleWritable(value));
        }
    }

    public static class AggregationReduceTask extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    	private double rankThreshold, distanceThreshold;
    	public void configure(JobConf job) {
    		this.distanceThreshold = Double.parseDouble(job.get("distanceThreshold"));
        	this.rankThreshold = Double.parseDouble(job.get("rankThreshold"));
        }
        public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
            double rank=0, distance=0;
            while(values.hasNext()){
            	double value = values.next().get();
            	if(value < 1.0)	
            		rank = value;
            	else
            		distance = value;
            }
            if(distance < distanceThreshold)
            	output.collect(key, new DoubleWritable(distance));
            else if(rank > rankThreshold)
            	output.collect(key, new DoubleWritable(rank));
        }
    }

    public JobConf createJob(String inputFiles, String outputFile) throws Exception {
    	JobConf job = new JobConf();
    	job.setJobName(this.getClass().getSimpleName());
    	job.setJarByClass(this.getClass());
    	
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        job.setMapperClass(AggregationMapTask.class);
        job.setReducerClass(AggregationReduceTask.class);
        
        job.setNumReduceTasks(8);
        String[] files = inputFiles.split(",");
        TextInputFormat.setInputPaths(job, new Path(files[0]), new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
    	
        return job;
    }

    public static void main(String[] args) throws Exception {
    	String inputFile = args[0];
    	String outputFile = args[1];
    	
    	Join agg = new Join();
    	JobConf job = agg.createJob(inputFile, outputFile);
    	
    	job.set("distanceThreshold", args[2]);
        job.set("rankThreshold", args[3]);
        
    	JobClient.runJob(job);
	}
}
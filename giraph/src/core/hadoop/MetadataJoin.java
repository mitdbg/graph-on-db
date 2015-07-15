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

public class MetadataJoin {
    public static class MetadataJoinMapTask extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text record, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        	String[] tokens = record.toString().split("\t",2);
        	int id = Integer.parseInt(tokens[0]);
        	output.collect(new IntWritable(id), new Text(tokens[1]));
        }
    }

    public static class MetadataJoinReduceTask extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
    	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String tuple = "";
            while(values.hasNext()){
            	tuple += values.next().toString();
            	if(values.hasNext())
            		tuple += "\t";
            }
            output.collect(key, new Text(tuple));
        }
    }

    public JobConf createJob(String inputFiles, String outputFile) throws Exception {
    	JobConf job = new JobConf();
    	job.setJobName(this.getClass().getSimpleName());
    	job.setJarByClass(this.getClass());
    	
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        job.setMapperClass(MetadataJoinMapTask.class);
        job.setReducerClass(MetadataJoinReduceTask.class);
        
        job.setNumReduceTasks(2);
        String[] files = inputFiles.split(",");
        TextInputFormat.setInputPaths(job, new Path(files[0]), new Path(files[1]));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));
    	
        return job;
    }

    public static void main(String[] args) throws Exception {
    	String inputFile = args[0];
    	String outputFile = args[1];
    	
    	MetadataJoin agg = new MetadataJoin();
    	JobConf job = agg.createJob(inputFile, outputFile);
    	
    	JobClient.runJob(job);
	}
}
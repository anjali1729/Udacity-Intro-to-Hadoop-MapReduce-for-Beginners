package com.mr.udacity.p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Total number of sales, total sales value from all stores. One Reducer.
 */
public class Q4 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "Q4");
	    job.setJarByClass(Q4.class);
	    
	    job.setMapperClass(Q4Map.class);
	    //job.setCombinerClass(Q4Reduce.class);
	    job.setReducerClass(Q4Reduce.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(DoubleWritable.class);
	    
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);

	}

}

class Q4Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	Text word = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] data=line.trim().split("\t");
		if(data.length==6){
			String storeName = "dummy";
			Double cost = Double.parseDouble(data[4]);
			word.set(storeName);
			con.write(word, new DoubleWritable(cost));
		}
	}
}

class Q4Reduce extends Reducer<Text, DoubleWritable, IntWritable, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
		double salesTotal = 0.0;
		int counter = 0;
		for(DoubleWritable value : values){
			counter++;
			salesTotal += value.get();
		}
	   con.write(new IntWritable(counter), new DoubleWritable(salesTotal));
	}
}

package com.mr.udacity.p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * breaking the sales down by store
 */

public class Q1 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Q1.class);
	    
	    job.setMapperClass(Q1Map.class);
	    job.setCombinerClass(Q1Reduce.class);
	    job.setReducerClass(Q1Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);

	}

}

class Q1Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	Text word = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] data=line.trim().split("\t");
		if(data.length==6){
			String storeName = data[2];
			Double cost = Double.parseDouble(data[4]);
			word.set(storeName);
			con.write(word, new DoubleWritable(cost));
		}
	}
}

class Q1Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
		Double sum = 0.0;
		for(DoubleWritable value : values){
		   sum += value.get();
		}
	   con.write(key, new DoubleWritable(sum));
	}
}

package com.mr.udacity.p2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * No. of hits to the page /assets/js/the-associates.js
 */

public class Q1 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "No. Of hits");
	    job.setJarByClass(Q1.class);
	    
	    job.setMapperClass(Q1Map.class);
	    //job.setCombinerClass(Q1Reduce.class);
	    job.setReducerClass(Q1Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);

	}

}

class Q1Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text word = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] data=line.trim().split(" ");
		if(data.length==10){
			String pageURL = data[6];
			word.set(pageURL);
			if(pageURL.equalsIgnoreCase("/assets/js/the-associates.js")){
				con.write(word, new IntWritable(1));
			}
		}
	}
}

class Q1Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable value : values){
		   sum += value.get();
		}
	   con.write(key, new IntWritable(sum));
	}
}

package com.mr.udacity.p2;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q3 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "No. Of hits");
	    job.setJarByClass(Q3.class);
	    
	    job.setMapperClass(Q3Map.class);
	    //job.setCombinerClass(Q1Reduce.class);
	    job.setReducerClass(Q3Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}

}

class Q3Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text word = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] data_temp=line.trim().split("\"");
		String[] data=data_temp[1].split(" ");
		if(data.length==3){
			String pageURL = data[1];
			if(pageURL.startsWith("http")){
				URL relativeURL = new URL(pageURL);
				word.set(relativeURL.getPath());
			}
			else{
				word.set(pageURL);
			}
			con.write(word, new IntWritable(1));
		}
	}
}

class Q3Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable value : values){
		   sum += value.get();
		}
	   con.write(key, new IntWritable(sum));
	}
}


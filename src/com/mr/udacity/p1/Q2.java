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
 * breaking the sales down by product
 */

public class Q2 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Q2.class);
	    
	    job.setMapperClass(Q2Map.class);
	    job.setCombinerClass(Q2Reduce.class);
	    job.setReducerClass(Q2Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);


	}

}

class Q2Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	Text word = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] data=line.trim().split("\t");		
		if(data.length==6){
			String productName = data[3];
			Double cost = Double.parseDouble(data[4]);
			word.set(productName);
			con.write(word, new DoubleWritable(cost));
		}

	}
}

class Q2Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
		Double sum = 0.0;
		for(DoubleWritable value : values){
		   sum += value.get();
		}
	   con.write(key, new DoubleWritable(sum));
	}
}


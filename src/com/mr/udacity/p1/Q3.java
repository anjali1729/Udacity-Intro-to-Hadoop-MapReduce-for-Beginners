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
 * highest sale for each store
 */
public class Q3 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(Q3.class);
	    
	    job.setMapperClass(Q3Map.class);
	    job.setCombinerClass(Q3Reduce.class);
	    job.setReducerClass(Q3Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);

	}

}

class Q3Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
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

class Q3Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
		Double maxCost = 0.0;
		Double currItemCost = 0.0;
		for(DoubleWritable value : values){
			currItemCost = value.get();
			if(maxCost < currItemCost){
				maxCost = currItemCost;
			}
		}
	   con.write(key, new DoubleWritable(maxCost));
	}
}

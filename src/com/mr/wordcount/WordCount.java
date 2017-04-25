package com.mr.wordcount;

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

public class WordCount {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    
	    job.setMapperClass(WordcountMap.class);
	    job.setCombinerClass(WordcountReduce.class);
	    job.setReducerClass(WordcountReduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);

	}

}

class WordcountMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] words=line.split(" ");
		for(String word: words )
		{
			Text outputKey = new Text(word.toUpperCase().trim());
			IntWritable outputValue = new IntWritable(1);
			con.write(outputKey, outputValue);
		}
	}
}

class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable value : values){
		   sum += value.get();
		}
	   con.write(word, new IntWritable(sum));
	}
}


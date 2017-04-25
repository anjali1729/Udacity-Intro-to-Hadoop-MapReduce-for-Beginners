package com.mr.udacity.patterns;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "Inverted Index");
	    job.setJarByClass(InvertedIndex.class);
	    
	    job.setMapperClass(InvertedIndexMap.class);
	    //job.setCombinerClass(Q1Reduce.class);
	    job.setReducerClass(InvertedIndexReduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}
}

class InvertedIndexMap extends Mapper<LongWritable, Text, Text, Text>{
	TreeMap<Blog, Text> ToRecordMap = new TreeMap<Blog , Text>(new BlogSizeCompare());
	Text word = new Text(); Text word2 = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] line_values=line.trim().split("\t");
		
		if(line_values.length==19){
			String blog_data=line_values[4];
			String node=line_values[0];
			String[] blog_words = blog_data.toLowerCase().split("[ .,!?\"()<>\\[\\]#$/=-]");
			int counter=0;
			word2.set(node);
			for(int i=0;i<blog_words.length;i++){
				word.set(blog_words[i]);
				con.write(word, word2);
			}
		}
	}             
}

class InvertedIndexReduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException{
		StringBuilder str = new StringBuilder();
		for (Text value : values) {
			String node = value.toString();
			str.append(node);
			if(values.iterator().hasNext()){
				str.append(",");
			}
		}
		con.write(key,new Text(str.toString()));
	}

}


package com.mr.udacity.patterns;

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


public class Blog1 {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "Filter Blog Posts");
	    job.setJarByClass(Blog1.class);
	    
	    job.setMapperClass(Blog1Map.class);
	    //job.setCombinerClass(Q1Reduce.class);
	    job.setReducerClass(Blog1Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}

}

class Blog1Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text word = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] line_values=line.trim().split("\t");
		
		if(line_values.length==19){
			String blog_data=line_values[4];
			String inputString = blog_data.replaceAll("\\<.*?\\>", ""); //REMOVE ALL HTML TAGS
			String noHTMLString=inputString.substring(1, inputString.length()-1); //REMOVE THE DOUBLE QUOTES AT BEGINNING AND END
			int fullStop = 0, questionMark = 0, exclamationMark = 0;
			int blogSize=noHTMLString.length();

			for(int i=0;i<blogSize;i++){
				if(noHTMLString.charAt(i) == '.'){
					fullStop++;
				}
				else if(noHTMLString.charAt(i) == '?'){
					questionMark++;
				}
				else if(noHTMLString.charAt(i) == '!'){
					exclamationMark++;
				}
			}
			
			int count = fullStop + questionMark + exclamationMark;
			
			if(count==0)
			{
				word.set(noHTMLString);
			}
			else if(count==1)
			{
				int lastIndex=blogSize-1;
				char lastChar=noHTMLString.charAt(lastIndex);
				if(lastChar == '.' || lastChar=='?' || lastChar=='!'){
					word.set(noHTMLString);
				}
			}
		}
		con.write(word, new IntWritable(1));
	}
	
	public void parseLine(){
		
	}
}

class Blog1Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable value : values){
		   sum += value.get();
		}
	   con.write(key, new IntWritable(sum));
	}
}


package com.mr.udacity.patterns;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

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


public class MeanSalesByWeekday {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "Mean Sales By Weekday");
	    job.setJarByClass(InvertedIndex.class);
	    
	    job.setMapperClass(MeanSalesByWeekdayMap.class);
	    //job.setCombinerClass(Q1Reduce.class);
	    job.setReducerClass(MeanSalesByWeekdayReduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}
}

class MeanSalesByWeekdayMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	Text word = new Text();
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] line_values=line.trim().split("\t");
		
		if(line_values.length==6){
			String salesDate=line_values[0];
			double salesValue=Double.parseDouble(line_values[4]);
			String weekDay = getWeekday(salesDate);
			word.set(weekDay);
			con.write(word, new DoubleWritable(salesValue));
		}
	}
	
	public String getWeekday(String salesDate){
		String dte = salesDate;
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = formatter.parse(dte);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		formatter = new SimpleDateFormat("EEEE"); //gets the weekday in String
		//System.out.println(formatter.format(date));
		return formatter.format(date);
	}
}

class MeanSalesByWeekdayReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context con) throws IOException, InterruptedException{
		double sum = 0.0;
		int counter = 0;
		double mean = 0.0;
		for(DoubleWritable value : values){
			sum += value.get();
			counter++;
		}
		mean = sum/counter;
		con.write(key,new DoubleWritable(mean));
	}

}



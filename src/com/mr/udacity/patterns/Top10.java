package com.mr.udacity.patterns;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top10 {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "Top 10");
	    job.setJarByClass(Top10.class);
	    
	    job.setMapperClass(Top10Map.class);
	    //job.setCombinerClass(Q1Reduce.class);
	    job.setReducerClass(Top10Reduce.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	}
}

class Top10Map extends Mapper<LongWritable, Text, NullWritable, Text>{
	TreeMap<Blog, Text> ToRecordMap = new TreeMap<Blog , Text>(new BlogSizeCompare());
	public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
		String line = value.toString();
		String[] line_values=line.trim().split("\t");
		
		if(line_values.length==19){
			String blog_data=line_values[4];
			String inputString = blog_data.replaceAll("\\<.*?\\>", ""); //REMOVE ALL HTML TAGS
			String noHTMLString=inputString.substring(1, inputString.length()-1); //REMOVE THE DOUBLE QUOTES AT BEGINNING AND END
			
			ToRecordMap.put(new Blog(noHTMLString), new Text(value));
		    
		    while(ToRecordMap.size()>10){
		    	ToRecordMap.pollLastEntry();
		    }
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		//return top 10
		System.out.println("Top 10 >> ");
       for (Text t:ToRecordMap.values()) {
    	   context.write(NullWritable.get(), t);
       }
       
       for (Blog t:ToRecordMap.keySet()) {
    	   System.out.println(t.getBlogContent() + " , " + t.getBlogContent().length());
       }
 
    }                 

}

class Top10Reduce extends Reducer<NullWritable, Text, NullWritable, Text>{
	public static TreeMap<Blog , Text> ToRecordMap = new TreeMap<Blog , Text>(new BlogSizeCompare());
	public void reduce(NullWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException{
		for (Text value : values) {
			String line = value.toString();
			String[] line_values=line.trim().split("\t");
			if (line.length() > 0) {
				String blog_data=line_values[4];
				String inputString = blog_data.replaceAll("\\<.*?\\>", ""); //REMOVE ALL HTML TAGS
				String noHTMLString=inputString.substring(1, inputString.length()-1); //REMOVE THE DOUBLE QUOTES AT BEGINNING AND END
				ToRecordMap.put(new Blog(noHTMLString), new Text(value));
			}
		}

		while (ToRecordMap.size() > 10) {
			ToRecordMap.pollLastEntry();
		}

		for (Text t : ToRecordMap.descendingMap().values()) {
			// Output our ten records to the file system with a null key
			con.write(NullWritable.get(), t);
		}
	}

}


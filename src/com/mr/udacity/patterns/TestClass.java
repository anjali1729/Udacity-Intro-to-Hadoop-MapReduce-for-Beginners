package com.mr.udacity.patterns;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

public class TestClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String line = "\"1729\"\t\"Whether pdf of Unit and Homework is available?\"\t\"cs101 pdf\"\t\"100000458\"\t\"<p><strong>Hello My Name is Anjali!<\\strong><\\p>\"\t\"question\"\t\"\n\"\t\"\n\"\t\"2012-02-25 08:09:06.787181+00\"\t\"1\"\t\"\"\t\"\n\"\t\"100000921\"\t\"2012-02-25 08:11:01.623548+00\"\t\"6922\"\t\"\n\"\t\"\n\"\t\"204\"\t\"f\"";
//		String[] line_values=line.trim().split("\t");
//		
//		for(int i=0;i<line_values.length;i++){
//			System.out.println("line_values["+i+"] = " + line_values[i]);
//		}
//		
//		String blog_data=line_values[4];
//		System.out.println("blog_data = "+blog_data);
//		
//		String noHTMLString = blog_data.replaceAll("\\<.*?>","");
//		System.out.println("noHTMLString = "+ noHTMLString);
//		int fullStop = 0, questionMark = 0, exclamationMark = 0;
//		int blogSize=noHTMLString.length();
//
//		for(int i=0;i<blogSize;i++){
//			if(noHTMLString.charAt(i) == '.'){
//				fullStop++;
//			}
//			else if(noHTMLString.charAt(i) == '?'){
//				questionMark++;
//			}
//			else if(noHTMLString.charAt(i) == '!'){
//				exclamationMark++;
//			}
//		}
//		
//		int count = fullStop + questionMark + exclamationMark;
//		System.out.println("int count = "+fullStop+"+" + questionMark +"+" + exclamationMark);
//		
//		if(count==0)
//		{
//			System.out.println("Empty string");
//		}
//		else if(count==1)
//		{
//			int lastIndex=blogSize-1;
//			char lastChar=noHTMLString.charAt(lastIndex);
//			if(lastChar == '.' || lastChar=='?' || lastChar=='!'){
//				System.out.println("Ends with . / ? / !");
//			}
//		}
		
//		TreeMap<Blog, Text> ToRecordMap = new TreeMap<Blog , Text>(new BlogSizeCompare());
//		ToRecordMap.put(new Blog("a"), new Text("ok"));
//		ToRecordMap.put(new Blog("aa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaaaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaaaaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaaaaaaaa"), new Text("ok"));
//		ToRecordMap.put(new Blog("aaaaaaaaaaaaa"), new Text("ok"));
//			    
//	    while(ToRecordMap.size()>10){
//	    	for (Entry<Blog, Text> ent : ToRecordMap.entrySet()) {
//	    	    int key = ent.getKey().getBlogContent().length();
//	    	    Text value = ent.getValue();
//
//	    	    System.out.printf("%s : %s\n", key, value);
//	    	}
//	    	ToRecordMap.pollLastEntry(); 
//	    }
//	}
		
//		String toSplit = "a+b-c*d/e=f[g]h z";
//		String[] splitted = toSplit.split("[ -\\[\\]+*/=]");
//		for (String split: splitted) {
//		    System.out.println(split);
//		}
//		
//		long[] x = {58673,7000047,40231,23898,39980,15556,1033658,20063,1034438,1010498,10008829,27209,2003840,1031110,
//				59895,1013991,1030230,51804,1034530,38820,1027001,6090,36163,1034525,1028457,41912,1034358,27711,2209,
//				40488,1000431,66083,52447,1007608,55132,1011252,1034207,1028874,1030729,1001506,66505,2011035,6031252,
//				2003241,1016815,2019574,1007853,1016835,5006500,6003097,5014087,6028672,1005894,6004507,1007576,1010131,
//				4000333,10004184,6018203,1016782,6016882,9003491,6006160,6028886,1013419,5005694,1032109,2004893,1008261,
//				3000238,2003508,5006788,1016994,3000468,5009899,11000246,5011288,2016461,6029575,6026982,6002673,9508,2018301,
//				62630,10000243,7000790,1032115,7003318,7001164,6018659,6002545,1032115,1006084,1008386,7005846,2017302,10003797,
//				10001673,7003022,9003233,6008386,1025636,2017301,10009417,9005356,9001403,48820,7003035,10004997,2002998,7003035,
//				1013270,1014021,6018124,6012077,8001941,7005815,12001776,6005344,7003035,12000437,9000864,38883,10000126,6007187,
//				10010957,10001915,8000280,10010453,56956,7001188,7002333,10003144};
//		 Arrays.sort(x);
//		 System.out.println(x.length);
//		 for (long number : x) {
//			 System.out.print(number + ",");
//		 }
		Calendar c = Calendar.getInstance();
		String dte = "2012-01-01";
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = formatter.parse(dte);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		formatter = new SimpleDateFormat("EEEE");
		//System.out.println(date);
        System.out.println(formatter.format(date));
		c.setTime(date);
		//System.out.println(c.get(Calendar.DAY_OF_WEEK));
		//int dayOfWeek = c.get(Calendar.DAY_OF_WEEK); 
		
		
//		String text = "<B><strong>I don't want this to be bold</strong><\\B>";
//        System.out.println(text);
//        text = text.replaceAll("\\<.*?\\>", "");
//        System.out.println(text);
}
}

package org.wikiAnalysis;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Task2 {


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			Pattern pattern=Pattern.compile("(.*?),\"?\\{\\{Infobox[\\s+]([\\w\\s]+)");
			Matcher matcher1; 
			String line = value.toString();
			String modkey,modvalue;
			matcher1 = pattern.matcher(line);
			if(matcher1.find()){
				Key=matcher1.group(2).trim()+",";
				Value=matcher1.group(1).trim()+",";
				output.collect(new Text(Key), new Text(Value));
			}
			
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String str="";
			while (values.hasNext()) {
			str+=values.next();	
				
			}
			
			output.collect(key, new Text(str));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task2.class);
		conf.setJobName("task2");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path("./input/"));
		FileOutputFormat.setOutputPath(conf, new Path("./output_task2_1/"));

		JobClient.runJob(conf);
	}
}
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

public class Task1Infobox {


	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private Text page_template = new Text("Pages with Templates");
		private Text without_template = new Text("Pages Without Templates");
		private Text Total_pages = new Text("Total Number of pages that have Infobox");
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			Pattern pattern=Pattern.compile("(.*?)\\{\\{Infobox(\\|\\w*|l\\s[^\\w+])");
			Matcher matcher1; 
			String line = value.toString();
			matcher1 = pattern.matcher(line);
			if(matcher1.find()){
				output.collect(without_template, IntWritable(1));
			}
			else{
				output.collect(page_template, IntWritable(1));
			}
			output.collect(Total_pages, IntWritable(1));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task1Infobox.class);
		conf.setJobName("task1");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path("./input/"));
		FileOutputFormat.setOutputPath(conf, new Path("./output_task1/"));

		JobClient.runJob(conf);
	}
}
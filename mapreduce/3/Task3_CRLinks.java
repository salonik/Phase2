package org.wikiAnalysis;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
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
import org.format.XmlInputFormat;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;




public class Task3_CRLinks {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable>output, Reporter reporter) throws IOException {
			String xmlString;
			SAXBuilder builder;
			Reader input;
			Document document;
			Element root;
			List<Element> EList= new ArrayList<Element>();
			xmlString = value.toString();
			builder = new SAXBuilder();
			input = new StringReader(xmlString);
			try {

				document = builder.build(in);
				root = document.getRootElement();
				Element revision=root.getChild("revision");
				EList =revision.getChildren("crosslanguage");
				Iterator<Element> iterator = elementList.iterator();
				while (iterator.hasNext()) {
					Element child = (Element) iterator.next();
					String CrossLanguage=child.getText().trim();
					output.collect(new Text(CrossLanuage),IntWritable(1));
					
				}
				
			} catch (JDOMException ex) {
				Logger.getLogger(Map.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IOException ex) {
				Logger.getLogger(Map.class.getName()).log(Level.SEVERE, null, ex);
			}

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
		JobConf conf = new JobConf(Task3_CRLinks.class);
		conf.setJobName("Task3_CRLinks");
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(conf, new Path("./input_Task3/"));
		FileOutputFormat.setOutputPath(conf, new Path("./output_Task3_CRLinks/"));

		JobClient.runJob(conf);
		
		
	}

}

package org.wikiAnalysis;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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





public class Task5 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
	
		public void map(LongWritable key, Text value, OutputCollector<Text,Text>output, Reporter reporter) throws IOException {
			String xmlString;
			String parsedTime_temp,parsedTime;
			String parsedTitle;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String currDate = dateFormat.format(new Date());

			String two= getPastDate(-60,dateFormat);
			
			String four= getPastDate(-120,dateFormat);
			
			String six= getPastDate(-180,dateFormat);
			
			String one_year= getPastDate(-365,dateFormat);
			
			String two_years= getPastDate(-730,dateFormat);
			
			String five_years= getPastDate(-1826,dateFormat);
			
			SAXBuilder builder;
			Reader input;
			Document document;
			Element element;
			

			xmlString = value.toString();
			builder = new SAXBuilder();
			input = new StringReader(xmlString);
			try {

				document = builder.build(input);
				element = document.getRootElement();
				Title =element.getChild("title").getText().trim() ;
				parsedTime_temp =element.getChild("revision").getChild("timestamp").getText().trim();
				RecencyDate=parsedTime_temp.split("T")[0];

				if((dateFormat.parse(RecencyDate).before(dateFormat.parse(currDate))&& dateFormat.parse(RecencyDate).after(dateFormat.parse(two)))||dateFormat.parse(RecencyDate).equals(dateFormat.parseObject(currDate)))
					output.collect(new Text("Most Recent"),new Text(parsedTitle));

				else if((dateFormat.parse(RecencyDate).before(dateFormat.parse(four)) && dateFormat.parse(RecencyDate).after(dateFormat.parse(two)))||dateFormat.parse(RecencyDate).equals(dateFormat.parseObject(two)))
					output.collect(new Text("Two Months old"),new Text(parsedTitle));

				else if((dateFormat.parse(RecencyDate).before(dateFormat.parse(six)) && dateFormat.parse(RecencyDate).after(dateFormat.parse(two)))||dateFormat.parse(RecencyDate).equals(dateFormat.parseObject(four)))
					output.collect(new Text("Four Months old"),new Text(parsedTitle));
				
				else if((dateFormat.parse(RecencyDate).before(dateFormat.parse(one_year)) && dateFormat.parse(RecencyDate).after(dateFormat.parse(six)))||dateFormat.parse(RecencyDate).equals(dateFormat.parseObject(six)))
					output.collect(new Text("Six Months old"),new Text(parsedTitle));

				else if((dateFormat.parse(RecencyDate).before(dateFormat.parse(two_years)) && dateFormat.parse(RecencyDate).after(dateFormat.parse(one_year)))||dateFormat.parse(RecencyDate).equals(dateFormat.parseObject(one_year)))
					output.collect(new Text("One Year"),new Text(parsedTitle));


				else if((dateFormat.parse(RecencyDate).before(dateFormat.parse(five_years)) && dateFormat.parse(RecencyDate).after(dateFormat.parse(two_years)))||dateFormat.parse(RecencyDate).equals(dateFormat.parseObject(two_years)))
					output.collect(new Text("Two years"),new Text(parsedTitle));

				
				
				else{
					System.out.println(parsedTime);
					output.collect(new Text("Not known"),new Text(parsedTitle));
				}


			} catch (JDOMException ex) {
				Logger.getLogger(Map.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IOException ex) {
				Logger.getLogger(Map.class.getName()).log(Level.SEVERE, null, ex);
			} catch (ParseException e) {
				
				e.printStackTrace();
			}

		}

		public String getPastDate(int days,SimpleDateFormat dateFormat) {
			Calendar addDate=Calendar.getInstance();
			addDate.add(Calendar.DATE,days);
			
			return (dateFormat.format(addDate.getTime()));

		}

	}
	public static class Task5Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text textkey, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String Page_name="";
			while (values.hasNext()) {
				Page_name+=values.next().toString()+",";
			}
			output.collect(textkey, new Text(Page_name));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Task5.class);
		conf.setJobName("Task5");
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
		conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Task5Reduce.class);
		conf.setReducerClass(Task5Reduce.class);
		conf.set("mapred.textoutputformat.separator", ",");
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(conf, new Path("./input_task3/"));
		FileOutputFormat.setOutputPath(conf, new Path("./output_task5/"));

		JobClient.runJob(conf);

	}

}

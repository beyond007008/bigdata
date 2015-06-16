package com.neusoft.mr;

import java.io.File;

import java.io.IOException;

import java.text.SimpleDateFormat;

import java.util.Date;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogAnalysiser extends Configured implements Tool {
	public static class ReduceClass extends MapReduceBase implements
			Reducer<Text, LongWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			Text newkey = new Text();
			newkey.set(key.toString().substring(
					key.toString().indexOf("::") + 1));
			LongWritable result = new LongWritable();
			long tmp = 0;
			int counter = 0;
			while (values.hasNext())// 累加同一个key的统计结果
			{
				tmp = tmp + values.next().get();

				counter = counter + 1;// 担心处理太久，JobTracker长时间没有收到报告会认为TaskTracker已经失效，因此定时报告一下
				if (counter == 1000) {
					counter = 0;
					reporter.progress();
				}
			}
			result.set(tmp);
			output.collect(newkey, result);// 输出最后的汇总结果
		}
	}

	public static class PartitionerClass implements
			Partitioner<Text, LongWritable> {
		public int getPartition(Text key, LongWritable value, int numPartitions) {
			if (numPartitions >= 2)// Reduce 个数，判断流量还是次数的统计分配到不同的Reduce
				if (key.toString().startsWith("logLevel::"))
					return 0;
				else if (key.toString().startsWith("moduleName::"))
					return 1;
				else
					return 0;
			else
				return 0;
		}

		public void configure(JobConf job) {
		}
	}

	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();// 没有配置RecordReader，所以默认采用line的实现，key就是行号，value就是行内容
			System.out.println("line==" + line);
			if (line == null || line.equals(""))
				return;
			String[] words = line.split(" ");
			if (words == null || words.length < 3)
				return;
			String logLevel = words[1];
			String moduleName = words[2];
			LongWritable recbytes = new LongWritable(1);
			Text record = new Text();
			record.set(new StringBuffer("logLevel::").append(logLevel)
					.toString());
			reporter.progress();
			output.collect(record, recbytes);// 输出日志级别统计结果，通过logLevel::作为前缀来标示。
			record.clear();
			record.set(new StringBuffer("moduleName::").append(moduleName)
					.toString());
			System.out.println("output key==" + record.toString());
			output.collect(record, new LongWritable(1));// 输出模块名的统计结果，通过moduleName::作为前缀来标示
		}
	}

	public static void main(String[] args) {
		try {
			int res;
			res = ToolRunner
					.run(new Configuration(), new LogAnalysiser(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.out.println("need inputpath and outputpath");
			return 1;
		}
		String inputpath = args[0];
		String outputpath = args[1];
		String shortin = args[0];
		String shortout = args[1];
		if (shortin.indexOf(File.separator) >= 0)
			shortin = shortin.substring(shortin.lastIndexOf(File.separator));
		if (shortout.indexOf(File.separator) >= 0)
			shortout = shortout.substring(shortout.lastIndexOf(File.separator));
		SimpleDateFormat formater = new SimpleDateFormat("yyyy.MM.dd");
		shortout = new StringBuffer(shortout).append("-")
				.append(formater.format(new Date())).toString();

		if (!shortin.startsWith("/"))
			shortin = "/" + shortin;
		if (!shortout.startsWith("/"))
			shortout = "/" + shortout;
		shortin = "/user/oracle/dfs/" + shortin;
		shortout = "/user/oracle/dfs/" + shortout;
		File inputdir = new File(inputpath);
		File outputdir = new File(outputpath);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.out.println("inputpath not exist or isn't dir!");
			return 0;
		}
		if (!outputdir.exists()) {
			new File(outputpath).mkdirs();
		}

		JobConf conf = new JobConf(getConf(), LogAnalysiser.class);// 构建Config
		// FileSystem fileSys = FileSystem.get(conf);
		// System.out.println("localDir=="+inputpath);
		// System.out.println("dfs dir=="+shortin);
		// fileSys.copyFromLocalFile(new Path(inputpath), new
		// Path(shortin));//将本地文件系统的文件拷贝到HDFS中

		conf.setJobName("analysisjob");
		conf.setOutputKeyClass(Text.class);// 输出的key类型，在OutputFormat会检查
		conf.setOutputValueClass(LongWritable.class); // 输出的value类型，在OutputFormat会检查
		// conf.setJarByClass(MapClass.class);
		// conf.setJarByClass(ReduceClass.class);
		// conf.setJarByClass(PartitionerClass.class);
		// conf.setJar("hadoopTest.jar");
		conf.setJarByClass(getClass());
		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(ReduceClass.class);
		conf.setPartitionerClass(PartitionerClass.class);
		conf.set("mapred.reduce.tasks", "2");// 强制需要有两个Reduce来分别处理流量和次数的统计
		FileInputFormat.setInputPaths(conf, shortin);// hdfs中的输入路径
		FileOutputFormat.setOutputPath(conf, new Path(shortout));// hdfs中输出路径

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		JobClient.runJob(conf);

		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		// 删除输入和输出的临时文件
		// fileSys.copyToLocalFile(new Path(shortout),new Path(outputpath));
		// fileSys.delete(new Path(shortin),true);
		// fileSys.delete(new Path(shortout),true);
		return 0;
	}

}

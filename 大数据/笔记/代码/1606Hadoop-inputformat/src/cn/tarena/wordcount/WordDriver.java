package cn.tarena.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class WordDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(WordDriver.class);
		job.setMapperClass(WordMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setInputFormatClass(AuthInputFormat.class);
		
		
		
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.234.22:9000/wordcount"));
		FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.234.22:9000/wordcount/result"));
		
		job.waitForCompletion(true);
		
	}
}

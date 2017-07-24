package cn.tarena.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * 写一个类，extends RecordReader。相当于实现自定义的输出读取器
 */
public class AuthRecordReader extends RecordReader<IntWritable, Text>{
	private FileSplit fs;
	private IntWritable key;
	private Text value;
	//注意，导的是org.apache.hadoop.util.LineReader;
	private LineReader reader;
	private int count=0;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		fs=(FileSplit) split;
		Path path=fs.getPath();
		Configuration conf=new Configuration();
		FileSystem system=path.getFileSystem(conf);
		//拿到文件的输入流，即文件数据
		FSDataInputStream in=system.open(path);
		reader=new LineReader(in);
		
	}
	/*
	 *1.这个方法会被多次调用，当此方法的返回值是true的时候，就会被调用一次。
	 *2.每当nextKeyValue()调用一次，getCurrentKey()和getCurrentValue()也会被调用一次
	 *3.getCurrentKey()每调用一次，会把这个方法的返回值交个map的输入key
	 *4.getCurrentValue()每调用一次，会把这个方法的返回值交个map的输入value
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key=new IntWritable();
		value=new Text();
		Text tmp=new Text();
		int result=reader.readLine(tmp);
		if(result==0){
			//证明没有行数据可读
			return false;
		}else{
			count++;
			key.set(count);
			
			//不能这么写：reader.readLine(value)，这么做，相当于读两行
			value=tmp;
			return true;
		}
		
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		if(reader!=null)reader.close();
		
	}

}

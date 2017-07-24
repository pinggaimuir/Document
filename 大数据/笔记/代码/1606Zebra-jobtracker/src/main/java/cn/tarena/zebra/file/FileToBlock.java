package cn.tarena.zebra.file;

import java.io.File;

import com.mchange.v2.c3p0.stmt.GooGooStatementCache;

import cn.tarena.zebra.common.GlobalEnv;
import rpc.domain.FileSplit;

/**
 * 这个类，是一个线程类，用于从文件队列里取出待处理的日志文件
 * 然后进行逻辑切块：用对象来封装一块数据，
 * 0，300000
 * 30000,30000
 * 60000,30000
 * 90000,12321
 * 
 * 
 * 物理切块：真切，把一个切块都存储成一个真实文件。
 * @author ysq
 *
 */
public class FileToBlock implements Runnable{

	@Override
	public void run() {
		try {
			while(true){
				//建议用take，因为会产生阻塞，当然用poll,利用null来做判断
				File file=GlobalEnv.getFileQueue().take();
				
				long length=file.length();
				//要考虑不整除的情况
				//本例中，按3MB切，最后应该是4块
				long blockNum=length%GlobalEnv.getBlocksize()==0?
						length/GlobalEnv.getBlocksize():
						length/GlobalEnv.getBlocksize()+1;
						
				//逻辑块的数据封装，用一个对象来封装，FileSplit,是Avro的可序列化对象
				//当每个FileSplit对象分装之后，存到队列里，供后续处理
				for(int i=1;i<=blockNum;i++){
					FileSplit split=new FileSplit();
					//相当于：D:\program\zebra\data\103_20150615143630_00_00_000.csv
					split.setPath(file.getPath());
					split.setStart((i-1)*GlobalEnv.getBlocksize());
					if(i==blockNum){
						split.setLength(length-split.getStart());
					}else{
						split.setLength(GlobalEnv.getBlocksize());
					}
					GlobalEnv.getSplitQueue().add(split);
					
					
					
				}
				
						
				
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

}

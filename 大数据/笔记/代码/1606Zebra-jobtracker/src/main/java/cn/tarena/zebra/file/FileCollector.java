package cn.tarena.zebra.file;

import java.io.File;

import cn.tarena.zebra.common.GlobalEnv;

/**
 * 这个类是一个线程类，用于定期扫面指定目录下的日志文件
 * 然后把日志文件放到队列里，供后续处理
 * @author ysq
 *
 */
public class FileCollector implements Runnable{

	@Override
	public void run() {
		try {
			while(true){
				File dir=new File(GlobalEnv.getDir());
				File[] files=dir.listFiles();
				//要考虑到日志被扫描到之后，要做处理，避免重复扫描
				//引入标识文件概念，扫描标识文件，根据标识文件找对应的日志文件
				//当日志文件扫描后，将日志文件删除，这样能够避免重复扫描
				for(File file:files){
					if(file.getName().endsWith(".ctr")){
						//103_20150615143630_00_00_000.csv
						String csvfilename=file.getName().split(".ctr")[0]+".csv";
						//得到日志文件
						File csvFile=new File(dir,csvfilename);
						//将日志文件存到队列里
						GlobalEnv.getFileQueue().add(csvFile);
						//将标识文件删除掉
						file.delete();
						
					}
					
				}
				Thread.sleep(GlobalEnv.getScannningInterval());
			}
			
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

}

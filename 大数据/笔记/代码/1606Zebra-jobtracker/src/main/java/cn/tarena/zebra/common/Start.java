package cn.tarena.zebra.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.tarena.zebra.file.FileCollector;
import cn.tarena.zebra.file.FileToBlock;
import cn.tarena.zebra.zk.ZkClientRunner;

public class Start {
	
	public static void main(String[] args) {
		System.out.println("jobtracker启动");
		ExecutorService service=Executors.newCachedThreadPool();
		service.execute(new FileCollector());
		service.execute(new FileToBlock());
		service.execute(new ZkClientRunner());
	}

}

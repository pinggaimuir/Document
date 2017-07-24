package cn.tarena.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cn.tarena.zebra.zk.ZkClientRunner;
import cn.tarena.zk.rpc.RpcServerRunner;

public class Start {

	public static void main(String[] args) {
		System.out.println("二级引擎启动");
		ExecutorService service=Executors.newCachedThreadPool();
		service.execute(new ZkClientRunner());
		service.execute(new RpcServerRunner());
		
	}
}

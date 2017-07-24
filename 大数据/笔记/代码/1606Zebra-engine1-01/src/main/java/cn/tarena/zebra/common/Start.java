package cn.tarena.zebra.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.ipc.RPCContext;

import cn.tarena.zebra.mapper.MapperRunner;
import cn.tarena.zebra.rpc.RpcClientRunner;
import cn.tarena.zebra.rpc.RpcServerRunner;
import cn.tarena.zebra.zk.ZkClientRunner;

public class Start {

	public static void main(String[] args) {
		System.out.println("一级引擎01启动");
		ExecutorService service=Executors.newCachedThreadPool();
		service.execute(new ZkClientRunner());
		service.execute(new RpcServerRunner());
		service.execute(new MapperRunner());
		service.execute(new RpcClientRunner());
	}
}

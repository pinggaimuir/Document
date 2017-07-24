package cn.tarena.zk.rpc;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;

import rpc.service.RpcSendHttpAppHost;

public class RpcServerRunner implements Runnable{

	@Override
	public void run() {
		NettyServer server=new NettyServer(
				new SpecificResponder(RpcSendHttpAppHost.class,new RpcSendHttpAppHostImpl()),
				new InetSocketAddress(8888));
		
	}

}

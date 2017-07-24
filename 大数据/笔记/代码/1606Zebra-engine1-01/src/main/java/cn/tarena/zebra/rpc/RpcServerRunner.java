package cn.tarena.zebra.rpc;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;

import cn.tarena.zebra.common.OwnEnv;
import rpc.service.RpcFileSplit;

public class RpcServerRunner implements Runnable{

	@Override
	public void run() {
		NettyServer server=new NettyServer(
				new SpecificResponder(RpcFileSplit.class,new RpcFileSplitImpl()), 
				new InetSocketAddress(OwnEnv.getRpcport()));
		
	}

}

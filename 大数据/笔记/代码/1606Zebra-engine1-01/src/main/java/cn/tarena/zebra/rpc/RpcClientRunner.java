package cn.tarena.zebra.rpc;

import java.net.InetSocketAddress;
import java.security.acl.Owner;
import java.util.Map;

import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.zookeeper.ZooKeeper;

import cn.tarena.zebra.common.GlobalEnv;
import cn.tarena.zebra.common.OwnEnv;
import rpc.domain.HttpAppHost;
import rpc.service.RpcSendHttpAppHost;

public class RpcClientRunner implements Runnable{

	@Override
	public void run() {
		try {
			ZooKeeper zk=GlobalEnv.connectZkServer();
			byte[]data=zk.getData(GlobalEnv.getEngine2path(),null,null);
			String ip=new String(data).split("/")[1];
			int port=Integer.parseInt(new String(data).split("/")[2]);
			NettyTransceiver client=new NettyTransceiver(
					new InetSocketAddress(ip, port));
			
			RpcSendHttpAppHost protocol=SpecificRequestor.getClient(
					RpcSendHttpAppHost.class, client);
			while(true){
				Map<CharSequence,HttpAppHost> map=OwnEnv.getMapQueue().take();
				protocol.sendHttpAppHostMap(map);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}

}

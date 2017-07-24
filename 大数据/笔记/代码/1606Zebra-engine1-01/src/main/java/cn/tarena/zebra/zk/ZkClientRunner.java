package cn.tarena.zebra.zk;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import cn.tarena.zebra.common.GlobalEnv;
import cn.tarena.zebra.common.OwnEnv;

/**
 * 这个类是个线程类，用于一级引擎向zk服务注册节点信息，
 * 比如自己机器ip信息，
 * rpc通信的服务端口等
 * @author ysq
 *
 */
public class ZkClientRunner implements Runnable{
	
	private static ZooKeeper zk;
	@Override
	public void run() {
		try {
			zk=GlobalEnv.connectZkServer();
			//engine1/node01
			String info=InetAddress.getLocalHost()+"/"+OwnEnv.getRpcport();
			zk.create(GlobalEnv.getEngine1path()+OwnEnv.getZnodepath(),
					info.getBytes(),Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL);
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
	}

	public static void setStatusFree() throws Exception {
		String info=InetAddress.getLocalHost()+"/"+OwnEnv.getRpcport()+"/free";
		zk.setData(GlobalEnv.getEngine1path()+OwnEnv.getZnodepath(),
				info.getBytes(), -1);
		
	}

	public static void setStatusBusy() throws Exception {
		//注意不要把ip和port更新丢
		String info=InetAddress.getLocalHost()+"/"+OwnEnv.getRpcport()+"/busy";
		zk.setData(GlobalEnv.getEngine1path()+OwnEnv.getZnodepath(),
				info.getBytes(), -1);
		
	}
	

}

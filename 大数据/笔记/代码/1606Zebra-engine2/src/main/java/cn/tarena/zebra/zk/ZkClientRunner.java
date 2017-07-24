package cn.tarena.zebra.zk;

import java.net.InetAddress;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import cn.tarena.zebra.common.GlobalEnv;

public class ZkClientRunner implements Runnable {

	@Override
	public void run() {
		try {
			ZooKeeper zk=GlobalEnv.connectZkServer();
			String info=InetAddress.getLocalHost()+"/8888";
			zk.create(GlobalEnv.getEngine2path(),info.getBytes(),
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
	}

}

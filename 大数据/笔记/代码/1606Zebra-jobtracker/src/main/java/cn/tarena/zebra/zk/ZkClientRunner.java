package cn.tarena.zebra.zk;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;

import cn.tarena.zebra.common.GlobalEnv;
import cn.tarena.zebra.rpc.RpcClientRunner;

/**
 * 这个是线程类，用于jobtracker向zk服务寻求信息
 * @author ysq
 *
 */
public class ZkClientRunner implements Runnable {

	@Override
	public void run() {
		try {
			ZooKeeper zk=GlobalEnv.connectZkServer();
			List<String> paths=zk.getChildren(GlobalEnv.getEngine1path(),null);
			//  /engine1/node01    /engine1/node01
			for(String childpath:paths){
				// 子路径并不是一个完整的路径： node01
				new Thread(new RpcClientRunner(childpath,zk)).start();;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
	}


}

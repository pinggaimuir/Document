package cn.tarena.zebra.rpc;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

import cn.tarena.zebra.common.GlobalEnv;
import rpc.domain.FileSplit;
import rpc.service.RpcFileSplit;

public class RpcClientRunner implements Runnable{
	// node01
	private String childpath;
	private ZooKeeper zk;
	
	public RpcClientRunner(String childpath, ZooKeeper zk) {
		this.childpath=childpath;
		this.zk=zk;
	}

	@Override
	public void run() {
		try {
			//这一步，相当于得到：sdfs/ip/port
			byte[] data=zk.getData(GlobalEnv.getEngine1path()+"/"+childpath,null,null);
			String ip=new String(data).split("/")[1];
			int port=Integer.parseInt(new String(data).split("/")[2]);
			
			NettyTransceiver client=new NettyTransceiver(
					new InetSocketAddress(ip, port));
			
			final RpcFileSplit protocol=SpecificRequestor.getClient(RpcFileSplit.class,client);
			FileSplit split=GlobalEnv.getSplitQueue().take();
			protocol.sendFileSplit(split);
			
			for(;;){
				final CountDownLatch  cdl=new CountDownLatch(1);
				zk.getData(GlobalEnv.getEngine1path()+"/"+childpath,new Watcher(){

					@Override
					public void process(WatchedEvent event) {
						if(event.getType()==EventType.NodeDataChanged){
							try {
								byte[] data=zk.getData(
										GlobalEnv.getEngine1path()+"/"+childpath,null,null);
								if(new String(data).contains("free")){
									//表示当前节点是空闲的，就从队列里拿出任务发给一级引擎
									FileSplit split=GlobalEnv.getSplitQueue().poll();
									if(split!=null){
										try {
											protocol.sendFileSplit(split);
										} catch (AvroRemoteException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
									}else{
										//证明任务队列已空，发完。
										//处理线索：向zk注册状态，让二级引擎能够观察到
									}
									
								}else{
									//如果繁忙，就不分任务
								}
							} catch (KeeperException | InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
							
							
							cdl.countDown();
						}
						
					}
					
				},null);
				cdl.await();
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
	}

}

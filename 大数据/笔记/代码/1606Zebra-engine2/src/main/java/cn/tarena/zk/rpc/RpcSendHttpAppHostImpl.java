package cn.tarena.zk.rpc;

import java.security.acl.Owner;
import java.util.Map;

import org.apache.avro.AvroRemoteException;

import cn.tarena.common.OwnEnv;
import rpc.domain.HttpAppHost;
import rpc.service.RpcSendHttpAppHost;

public class RpcSendHttpAppHostImpl implements RpcSendHttpAppHost{

	@Override
	public Void sendHttpAppHost(HttpAppHost httpAppHost) throws AvroRemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Void sendHttpAppHostMap(Map<CharSequence, HttpAppHost> hahMap) throws AvroRemoteException {
		//收到map后，存放到二级引擎的Map队列里
		System.out.println("二级引擎收到map："+hahMap.size());
		OwnEnv.getMapQueue().add(hahMap);
		//作业：二级的最后归并工作，什么时候开始，
		//判断的依据：
		//1.一级引擎的状态肯定处于free状态
		//2.jobTracker的任务队列里没有任务可发了。
		if(OwnEnv.getMapQueue().size()==4){
			//做最后的归并,如何将多个Map合并成一个map，先自己思考，
			//具体的实现代码在下面的ReducerRunner类
			new Thread(new ReducerRunner()).start();
		}
		return null;
	}

}

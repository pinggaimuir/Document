package cn.tarena.zebra.rpc;

import org.apache.avro.AvroRemoteException;

import cn.tarena.zebra.common.OwnEnv;
import rpc.domain.FileSplit;
import rpc.service.RpcFileSplit;

public class RpcFileSplitImpl implements RpcFileSplit{
	
	@Override
	public Void sendFileSplit(FileSplit fileSplit) throws AvroRemoteException {
		//一级引擎收到任务后，存到一级引擎的队列里
		System.out.println("一级引擎收到："+fileSplit);
		OwnEnv.getSpiltQueue().add(fileSplit);
		return null;
	}

}

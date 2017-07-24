package cn.tarena.zk.rpc;


import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import cn.tarena.common.OwnEnv;
import cn.tarena.zebra.db.ZebraDB;
import rpc.domain.HttpAppHost;

/**
 * 这个类，就是做所有map的合并
 * @author ysq
 *
 */
public class ReducerRunner implements Runnable{
	
	private static Map<String,HttpAppHost> map=new HashMap<String, HttpAppHost>();

	@Override
	public void run() {
		
		while(true){
			Map<CharSequence,HttpAppHost> reducemap=OwnEnv.getMapQueue().poll();
			if(reducemap==null){
				break;
			}else{
				for(Entry<CharSequence,HttpAppHost> entry:reducemap.entrySet()){
					HttpAppHost hah=entry.getValue();
					
					CharSequence key=hah.getReportTime() + "|" + hah.getAppType() + "|" + hah.getAppSubtype() + "|" + hah.getUserIP() + "|" + hah.getUserPort() + "|" + hah.getAppServerIP() + "|" + hah.getAppServerPort() +"|" + hah.getHost() + "|" + hah.getCellid();
					if(map.containsKey(key)){
						HttpAppHost mapHah=map.get(key);
						mapHah.setAccepts(mapHah.getAccepts()+hah.getAccepts());
						mapHah.setAttempts(mapHah.getAttempts()+hah.getAttempts());
						mapHah.setTrafficUL(mapHah.getTrafficUL()+hah.getTrafficUL());
						mapHah.setTrafficDL(mapHah.getTrafficDL()+hah.getTrafficDL());
						mapHah.setRetranUL(mapHah.getRetranUL()+hah.getRetranUL());
						mapHah.setRetranDL(mapHah.getRetranDL()+hah.getRetranDL());
						mapHah.setTransDelay(mapHah.getTransDelay()+hah.getTransDelay());
						
					}else{
						map.put(key.toString(), hah);
					}
				}
			}
		}
		//代码走到这，map里就含有所有的处理后的数据。然后做数据的落地
		
		System.out.println("reduce合并后的map总大小:"+map.size());
		//zebraDb是一个工具类，用于将数据落地到数据库里，所以需要引入c3p0配置文件，我已引入好
		//数据库的建表语句在zebra第五天资料里有，数据落地不是重点，可以不做
		ZebraDB.toDb(map);
	}

}

package cn.tarena.zebra.mapper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import cn.tarena.zebra.common.OwnEnv;
import cn.tarena.zebra.zk.ZkClientRunner;
import rpc.domain.FileSplit;
import rpc.domain.HttpAppHost;

/**
 * 1.从任务队列取任务，取Split对象
 * 2.取出path，通过path找到要处理的文件
 * 3.start 和length 确定处理哪一块的数据
 * 4.按行读取，然后按 | 切。然后根据zebra业务要求做数据封装，封装到对象
 * 
 * @author ysq
 *
 */
public class MapperRunner implements Runnable{

	@Override
	public void run() {
		try {
			while(true){
				FileSplit split=OwnEnv.getSpiltQueue().take();
				//代码走到这，说明接到有任务要处理,说明要进入繁忙状态，
				//这个状态可以向zk服务进行注册更新
				ZkClientRunner.setStatusBusy();
				Map<CharSequence,HttpAppHost> map=new HashMap<>();
				
				String path=split.getPath().toString();
				File file=new File(path);
				long start=split.getStart();
				long end=start+split.getLength();
				
				RandomAccessFile raf=new RandomAccessFile(file,"rw");
				FileChannel fc=raf.getChannel();
				//0,30000
				//3000,6000
				//注意，要考虑到，位置的处理，向前追溯或向后追溯都可以处理
				if(start==0){
					
				}else{
					long headposition=start;
					while(true){
						fc.position(headposition);
						ByteBuffer tmp=ByteBuffer.allocate(1);
						fc.read(tmp);
						if(new String(tmp.array()).equals("\n")){
							start=headposition+1;
							//找到位置之后，别忘了break
							break;
						}else{
							headposition=headposition-1;
						}
					}
				}
				
				if(end==file.length()){
					
				}else{
					long tailpostion=end;
					while(true){
						fc.position(tailpostion);
						ByteBuffer tmp=ByteBuffer.allocate(1);
						fc.read(tmp);
						if(new String(tmp.array()).equals("\n")){
							end=tailpostion;
							break;
						}else{
							tailpostion=tailpostion-1;
						}
						
					}
				}
				//代码走到这，end和start都处理完毕
				ByteBuffer buffer=ByteBuffer.allocate((int) (end-start));
				
				//****注意，文件通道在读取数据前，别忘了设置读取指针。
				fc.position(start);
				while(buffer.hasRemaining()){
					fc.read(buffer);
				}
				//代码走到这，buffer就是待处理的数据
				BufferedReader br=new BufferedReader(
						new InputStreamReader(
								new ByteArrayInputStream(buffer.array())));
				//按行读取，
				String line=null;
				while((line=br.readLine())!=null){
					//截串工作，
					String[] data=line.split("\\|");
					//得到当前日志文件的日期
					String reportTime=path.split("_")[1];
					HttpAppHost hah=new HttpAppHost();
					hah.setReportTime(reportTime);
					//上网小区的id
					hah.setCellid(data[16]);
					//应用类
					hah.setAppType(Integer.parseInt(data[22]));
					//应用子类
					hah.setAppSubtype(Integer.parseInt(data[23]));
					//用户ip
					hah.setUserIP(data[26]);
					//用户port
					hah.setUserPort(Integer.parseInt(data[28]));
					//访问的服务ip
					hah.setAppServerIP(data[30]);
					//访问的服务port
					hah.setAppServerPort(Integer.parseInt(data[32]));
					//域名
					hah.setHost(data[58]);
					
					int appTypeCode=Integer.parseInt(data[18]);
					String transStatus=data[54];
					
					//业务逻辑处理
					//小区id如果没有，就补全9个0
					if(hah.getCellid()==null||hah.getCellid().equals("")){
						hah.setCellid("000000000");
					}
					//如果=103，尝试次数就设置为1
					if(appTypeCode==103){
						hah.setAttempts(1);
					}
					//如果=103，并且状态码是低下的数字，接收次数就设置为1
					if(appTypeCode==103&&"10,11,12,13,14,15,32,33,34,35,36,37,38,48,49,50,51,52,53,54,55,199,200,201,202,203,204,205,206,302,304,306".contains(transStatus)){
						hah.setAccepts(1);
					}else{
						hah.setAccepts(0);
					}
					//如果=103，就设置上传流量
					if(appTypeCode == 103){
						hah.setTrafficUL(Long.parseLong(data[33]));
					}
					//如果=103，就设置下传流量
					if(appTypeCode == 103){
						hah.setTrafficDL(Long.parseLong(data[34]));
					}
					//如果=103，设置重传上传流量
					if(appTypeCode == 103){
						hah.setRetranUL(Long.parseLong(data[39]));
					}
					//如果=103，设置重传下行流量
					if(appTypeCode == 103){
						hah.setRetranDL(Long.parseLong(data[40]));
					}
					//设置传输延时，请求终止时间-请求的起始时间。
					if(appTypeCode==103){
						hah.setTransDelay(Long.parseLong(data[20]) - Long.parseLong(data[19]));
					}
					//后期要把封装好的Map<CharSequence,HttpAppHost>通过rpc传给二级引擎。
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
						map.put(key,hah);
					}
				}
				//步骤9:
				//代码走到这，说明当前的一级引擎任务处理完毕。把map存到阻塞队列里，供后续的rpc调用
				OwnEnv.getMapQueue().put(map);
				//处理完之后，应该更新下当前的节点状态,一级引擎空闲，
				ZkClientRunner.setStatusFree();
				
				System.out.println(map.size());
				
			}
		} catch (Exception e) {
			
		}
		
	}

}

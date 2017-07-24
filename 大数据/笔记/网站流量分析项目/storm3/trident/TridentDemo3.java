package cn.tedu.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
/**
 * 聚合操作案例
 */
public class TridentDemo3 {
	public static void main(String[] args) {
		//--创建topology
		TridentTopology topology = new TridentTopology();
		
		//TODO
		Stream s = topology.newStream("xx", new SentenceSpout())
				.aggregate(new Fields("name"),new SentenceAggerator(),new Fields("name","count"))
			;
			
			s.each(s.getOutputFields(), new PrintFilter());
		
		
		//--提交到集群中运行
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("MyTopology", conf, topology.build());
		
		//--运行10秒钟后杀死Topology关闭集群
		Utils.sleep(1000 * 10);
		cluster.killTopology("MyTopology");
		cluster.shutdown();
		
	}
}

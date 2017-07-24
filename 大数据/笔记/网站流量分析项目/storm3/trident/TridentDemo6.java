package cn.tedu.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
/**
 * State存储数据案例
 */
public class TridentDemo6 {
	public static void main(String[] args) {
		//--创建topology
		TridentTopology topology = new TridentTopology();
		

		/**
		 * 更新数据到State 
		 * 	可以将数据持久化到State指定的位置
		 * 	State对象可以提供以后需要时的查询
		 */
		Stream s = topology.newStream("xx", new SentenceSpout())
			.partitionAggregate(new Fields("name"), new SentenceAggerator(), new Fields("name","count"))	
			;
		TridentState state = s.partitionPersist(new MyStateFactory(), new Fields("name","count"), new MyStateUpdater());
		
		/**
		 * 查询state
		 */
		FixedBatchSpout spout2 = new FixedBatchSpout(new Fields("qname"), 1, new Values("xiaoming"),new Values("xiaohua"));
		spout2.setCycle(true);
		Stream s2 = topology.newStream("yy",spout2);
		s2 = s2.stateQuery(state, new Fields("qname"), new MyStateQuery(), new Fields("name","count"));
		s2.each(s2.getOutputFields(), new PrintFilter());
		
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

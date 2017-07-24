package cn.tedu.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.Stream;
import storm.trident.TridentTopology;
/**
 * 合并操作案例
 */
public class TridentDemo5 {
	public static void main(String[] args) {
		//--创建topology
		TridentTopology topology = new TridentTopology();
		
		/**
		 * merge
		 */
//		Stream s1 = topology.newStream("xx", new SentenceSpout());
//		Stream s2 = topology.newStream("yy", new SentenceSpout());
//		Stream s3 = topology.newStream("zz", new SentenceSpout());
//		Stream s = topology.merge(s1,s2,s3);
//		s.each(s.getOutputFields(), new PrintFilter());

		Stream s1 = topology.newStream("xx", new SentenceSpout());
		Stream s2 = topology.newStream("yy", new GenderSpout());
		Stream s = topology.join(s1, new Fields("name"), s2, new Fields("name"),new Fields("name","sentence","gender"));
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

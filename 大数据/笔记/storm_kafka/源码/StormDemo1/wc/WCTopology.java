package cn.teud.storm.wc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WCTopology {
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";
	
	public static void main(String[] args) throws Exception {
		//--创建对象
		SententSpout spout = new SententSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt wcBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		//--创建topology构建者
		TopologyBuilder builder = new TopologyBuilder();
		
		//--告知构造者topology的结构
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, wcBolt).fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
		
		//--通过构造者创建topology
		StormTopology topology = builder.createTopology();
		
		//--上传topology到集群中运行 -- 集群运行时的写法
		//Config conf = new Config();
		//StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, topology);
		
		//--执行topology -- 单机测试时的写法
		//----创建LocalCluster对象在本地模拟一个集群
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, conf, topology);
		
		
		//--让集群运行10秒后，自动退出
		Thread.sleep(1000 * 10);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();
	}
}

package cn.tedu.flux;

import java.util.UUID;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class FluxTopology {
	public static void main(String[] args) {
		/**
		 * 实时计算pv uv vv newip newcust
		 */
		
		//SPOUT的id 要求唯一
		String KAFKA_SPOUT_ID = "flux_spout";
		//要连接的kafka的topic
		String CONSUME_TOPIC = "flux_topic";
		//要连接的zookeeper的地址
		String ZK_HOSTS = "192.168.242.101:2181"; 

		//设定连接服务器的参数
		BrokerHosts hosts = new ZkHosts(ZK_HOSTS);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, CONSUME_TOPIC, "/" + CONSUME_TOPIC, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
	   
		//从kafka读取数据发射
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(KAFKA_SPOUT_ID, kafkaSpout);
		//清理数据
		builder.setBolt("ClearBolt", new ClearBolt()).shuffleGrouping(KAFKA_SPOUT_ID);
		//计算pv
		builder.setBolt("PvBolt", new PVBolt()).shuffleGrouping("ClearBolt");
		//计算uv
		builder.setBolt("UvBolt", new UvBolt()).shuffleGrouping("PvBolt");
		//计算vv
		builder.setBolt("VvBolt", new VvBolt()).shuffleGrouping("UvBolt");
		//计算newip
		builder.setBolt("NewIpBolt", new NewIpBolt()).shuffleGrouping("VvBolt");
		//计算newcust
		builder.setBolt("NewCustBolt", new NewCustBolt()).shuffleGrouping("NewIpBolt");
		//将结果数据落地到数据库中
		builder.setBolt("ToMysqlBolt", new ToMysqlBolt()).shuffleGrouping("NewCustBolt");
		
		builder.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("NewCustBolt");
		//将数据持久化到hbase中间存储中，方便后续使用
		builder.setBolt("ToHBaseBolt", new ToHbaseBolt()).shuffleGrouping("NewCustBolt");
		
		StormTopology topology = builder.createTopology();
		
		/**
		 * 每隔5分钟计算br avgtime avgdeep
		 */
		TopologyBuilder builder2 = new TopologyBuilder();
		builder2.setSpout("flux_spout2", new TimeSpout());
		builder2.setBolt("br_bolt", new BrBolt()).shuffleGrouping("flux_spout2");
		//TODO:计算avgTime
		//TODO:计算avgDeep
		//TODO:写入mysql
		builder2.setBolt("PrintBolt2", new PrintBolt2()).shuffleGrouping("br_bolt");
		StormTopology topology2 = builder2.createTopology();
		
		/**
		 * 提交Topology给集群运行
		 */
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("MyTopology", conf, topology);
		cluster.submitTopology("MyTopology2", conf, topology2);
		//--运行10秒钟后杀死Topology关闭集群
		Utils.sleep(1000 * 1000);
		cluster.killTopology("MyTopology"); 
		cluster.killTopology("MyTopology2"); 
		cluster.shutdown();
	}
}

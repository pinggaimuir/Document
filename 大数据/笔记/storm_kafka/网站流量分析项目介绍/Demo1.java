package cn.tedu.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;;

public class Demo1 {
	
	@Test
	public void producer(){
		Properties prop = new Properties();
		prop.put("serializer.class", "kafka.serializer.StringEncoder");
		prop.put("metadata.broker.list", "192.168.242.101:9092,192.168.242.102:9092,192.168.242.103:9092");
		Producer<Integer, String> producer = new Producer<>(new ProducerConfig(prop));
		producer.send(new KeyedMessage<Integer, String>("topicx1", "hello kafka from java!"));
	}
	
	
	@Test
	public void Consumer(){
		Properties prop = new Properties();  
		prop.put("zookeeper.connect", "192.168.242.101:2181,192.168.242.102:2181,192.168.242.103:2181");//声明zk  
		prop.put("group.id", "gx1");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据  
		prop.put("auto.offset.reset", "smallest");
		
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(prop));
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();  
		topicCountMap.put("topicx1", 1); // 一次从主题中获取一个数据  
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(topicCountMap);
		
		KafkaStream<byte[], byte[]> kafkaStream = streams.get("topicx1").get(0);
		
		ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
		
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> next = it.next();
			byte [] data = next.message();
			String str = new String(data);
			System.out.println(str);
		}
	}
}

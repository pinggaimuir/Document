package cn.tedu.trident;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout{

	private SpoutOutputCollector collector = null;
	
	private Values [] values = {
			new Values("xiaoming","i am so shuai"),
			new Values("xiaoming","do you like me"),
			new Values("xiaohua","i do not like you"),
			new Values("xiaohua","you look like fengjie"),
			new Values("xiaoming","are you sure you do not like me"),
			new Values("xiaohua","yes i am"),
			new Values("xiaoming","ok i am sure")
	};
	
	private int index = 0;
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if(index<values.length){
			collector.emit(values[index]);
			index++;
		}
		Utils.sleep(100);
	}
//	@Override
//	public void nextTuple() {
//		collector.emit(values[index]);
//		index = index+1 == values.length ? 0 : index+1;
//		Utils.sleep(100);
//	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields("name","sentence");
		declarer.declare(fields);
	}

}

package cn.tedu.trident;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StateQuerySpout extends BaseRichSpout{

	private SpoutOutputCollector collector = null;
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	boolean flag = true;
	@Override
	public void nextTuple() {
		try {
			if(flag){
				collector.emit(new Values("xiaoming"));
//				flag =false;
//			}else{
//				collector.emit(new Values("xiaohua"));
//				flag =true;
			}
			Thread.sleep(10);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("qname"));
	}

}

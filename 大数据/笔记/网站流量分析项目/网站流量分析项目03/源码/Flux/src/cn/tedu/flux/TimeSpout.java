package cn.tedu.flux;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TimeSpout extends BaseRichSpout {

	private SpoutOutputCollector collector = null;
	private long time = 0l;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		time = System.currentTimeMillis();
	}

	@Override
	public void nextTuple() {
		try {
			long now = System.currentTimeMillis();
			if(now - time >= 1000 * 10 /** 5*/){
				collector.emit(new Values(now));
				time = now;
			}else{
				Thread.sleep(1);
				return;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("signal"));
	}

}

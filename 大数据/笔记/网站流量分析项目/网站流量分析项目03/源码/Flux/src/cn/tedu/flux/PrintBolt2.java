package cn.tedu.flux;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PrintBolt2 extends BaseRichBolt{

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Fields fields = input.getFields();
		StringBuffer buf = new StringBuffer();
		Iterator<String> it = fields.iterator();
		while(it.hasNext()){
			String key = it.next();
			Object value = input.getValueByField(key);
			buf.append("----"+key+":"+value+"-----");
		}
		System.out.println(buf.toString());
		
		collector.emit(input.getValues());
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("br"));
	}
	
}

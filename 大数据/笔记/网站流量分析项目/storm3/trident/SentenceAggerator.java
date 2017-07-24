package cn.tedu.trident;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SentenceAggerator extends BaseAggregator<ConcurrentHashMap<String,Integer>> {

	@Override
	public ConcurrentHashMap<String,Integer> init(Object batchId, TridentCollector collector) {
		return new ConcurrentHashMap<String,Integer>();
	}

	@Override
	public void aggregate(ConcurrentHashMap<String,Integer> val, TridentTuple tuple, TridentCollector collector) {
		String name = tuple.getStringByField("name");
		val.put(name, val.containsKey(name) ? val.get(name)+1 : 1);
	}

	@Override
	public void complete(ConcurrentHashMap<String,Integer> val, TridentCollector collector) {
		for(Map.Entry<String, Integer> entry : val.entrySet()){
			collector.emit(new Values(entry.getKey(),entry.getValue()));
		}
	}

}

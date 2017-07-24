package cn.tedu.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class CountAggerator extends BaseAggregator<Integer> {

	int count = 0;
	@Override
	public Integer init(Object batchId, TridentCollector collector) {
		return null;
	}

	@Override
	public void aggregate(Integer val, TridentTuple tuple, TridentCollector collector) {
		count++;
	}

	@Override
	public void complete(Integer val, TridentCollector collector) {
		collector.emit(new Values(count));
	}
	
}

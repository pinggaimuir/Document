package cn.tedu.trident;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

public class MyStateQuery extends BaseQueryFunction<MyState, Integer> {

	@Override
	public List<Integer> batchRetrieve(MyState state, List<TridentTuple> args) {
		List<Integer> list = new ArrayList<Integer>();
		for(TridentTuple arg : args){
			int count = state.get(arg.getStringByField("qname"));
			list.add(count);
		}
		return list;
	}

	@Override
	public void execute(TridentTuple tuple, Integer result, TridentCollector collector) {
		collector.emit(new Values(tuple.getStringByField("qname"),result));
	}
	
}

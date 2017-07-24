package cn.tedu.trident;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

public class MyStateUpdater extends BaseStateUpdater<MyState> {

	@Override
	public void updateState(MyState state, List<TridentTuple> tuples, TridentCollector collector) {
		for(TridentTuple tuple : tuples){
			state.put(tuple.getStringByField("name"), tuple.getIntegerByField("count"));
		}
	}

}

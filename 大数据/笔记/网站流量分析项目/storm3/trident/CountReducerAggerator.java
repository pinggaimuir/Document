package cn.tedu.trident;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class CountReducerAggerator implements ReducerAggregator<Integer> {

	@Override
	public Integer init() {
		return 0;
	}

	@Override
	public Integer reduce(Integer curr, TridentTuple tuple) {
		return curr+1;
	}

}

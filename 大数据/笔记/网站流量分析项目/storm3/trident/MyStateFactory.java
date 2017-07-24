package cn.tedu.trident;

import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;


public class MyStateFactory implements StateFactory {

	private static MyState state = new MyState();
	
	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		return state;
	}
}

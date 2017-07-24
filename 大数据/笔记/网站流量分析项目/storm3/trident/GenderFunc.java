package cn.tedu.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class GenderFunc extends BaseFunction{

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String name = tuple.getStringByField("name");
		if("xiaoming".equals(name)){
			collector.emit(new Values("male"));
		}else if("xiaohua".equals(name)){
			collector.emit(new Values("female"));
		}else{
		}
	}
	
}

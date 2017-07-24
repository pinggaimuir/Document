package cn.tedu.trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class XiaohuaFilter extends BaseFilter {

	@Override
	public boolean isKeep(TridentTuple tuple) {
		return !"xiaohua".equals(tuple.getStringByField("name"));
	}

}

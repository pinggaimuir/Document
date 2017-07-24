package cn.tedu.trident;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.tuple.Fields;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class PrintFilter extends BaseFilter{

	private TridentOperationContext context = null;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		this.context = context;
	}
	
	@Override
	public boolean isKeep(TridentTuple tuple) {
		StringBuffer buf = new StringBuffer();
		buf.append("---partition_id:"+context.getPartitionIndex());
		
		Fields fields = tuple.getFields();
		Iterator<String> it = fields.iterator();

		while(it.hasNext()){
			String key = it.next();
			Object value = tuple.getValueByField(key);
			buf.append("---"+key+":"+value+"---");
		}
		System.out.println(buf.toString());
		
		return true;
	}
	
}

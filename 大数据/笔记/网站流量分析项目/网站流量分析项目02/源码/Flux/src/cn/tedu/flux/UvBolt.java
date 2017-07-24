package cn.tedu.flux;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.thrift.generated.Hbase;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.tedu.dao.HBaseDao;
import cn.tedu.domain.FluxInfo;

public class UvBolt extends BaseRichBolt {
	
	private OutputCollector collector = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}


	@Override
	public void execute(Tuple input) {
		List<Object> values = input.getValues();
		//如果uv_id在今天的其他数据中没有出现过，则输出1，否则输出0
		List<FluxInfo> list = HBaseDao.queryData("^"+input.getStringByField("time")+"_"+input.getStringByField("uv_id")+"_.*$");
		values.add(list.size() == 0 ? 1 : 0);
		collector.emit(new Values(values.toArray()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time","uv_id","ss_id","ss_time","urlname","cip","pv","uv"));		
	}

}

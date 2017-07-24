package cn.tedu.flux;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.tedu.dao.HBaseDao;
import cn.tedu.domain.FluxInfo;
import cn.tedu.flux.utils.FluxUtils;

public class BrBolt extends BaseRichBolt {

	private OutputCollector collector = null;
	private Map<String,Integer> map = new HashMap<>();
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long time = input.getLongByField("signal");
		Date date = new Date(time);
		String dateStr = FluxUtils.formatDate(date);
		//如果uv_id在今天的其他数据中没有出现过，则输出1，否则输出0
		List<FluxInfo> list = HBaseDao.queryData("^"+dateStr+"_.*$");
		//遍历数据
		for(FluxInfo fi : list){
			String ss_id = fi.getSs_id();
			map.put(ss_id, map.containsKey(ss_id) ? map.get(ss_id)+1 : 1);
		}
		//计算跳出率
		int ssCount = map.size();
		int brCount = 0;
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			if(entry.getValue() == 1)brCount++;
		}
		double br = Math.round(brCount * 100.0 / ssCount)/100.0;
		
		//输出结果
		collector.emit(new Values(br));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("br"));
	}

}

package cn.tedu.flux;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tedu.dao.HBaseDao;
import cn.tedu.domain.FluxInfo;

public class ToHbaseBolt extends BaseRichBolt {

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
	}

	@Override
	public void execute(Tuple input) {
		FluxInfo fi = new FluxInfo();
		fi.setTime(input.getStringByField("time"));
		fi.setUv_id(input.getStringByField("uv_id"));
		fi.setSs_id(input.getStringByField("ss_id"));
		fi.setSs_time(input.getStringByField("ss_time"));
		fi.setUrlname(input.getStringByField("urlname"));
		fi.setCip(input.getStringByField("cip"));
		HBaseDao.insertData(fi);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
}

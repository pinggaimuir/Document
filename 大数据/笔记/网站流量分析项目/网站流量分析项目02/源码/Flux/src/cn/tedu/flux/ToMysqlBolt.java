package cn.tedu.flux;

import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.tedu.dao.MysqlDao;
import cn.tedu.domain.ResultInfo;
import cn.tedu.flux.utils.FluxUtils;

public class ToMysqlBolt extends BaseRichBolt {

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {
		ResultInfo ri = new ResultInfo();
		
		String time = input.getStringByField("time");
		Date date = FluxUtils.parseDateStr(time);
		ri.setTime(new java.sql.Date(date.getTime()));
		ri.setPv(input.getIntegerByField("pv"));
		ri.setUv(input.getIntegerByField("uv"));
		ri.setVv(input.getIntegerByField("vv"));
		ri.setNewip(input.getIntegerByField("newip"));
		ri.setNewcust(input.getIntegerByField("newcust"));
	
		MysqlDao.insert(ri);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}

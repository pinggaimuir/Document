package cn.tedu.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import cn.tedu.domain.FluxInfo;
import cn.tedu.flux.utils.FluxUtils;

public class HBaseDao {
	private HBaseDao() {
	}
	
	public static List<FluxInfo> queryData(String reg){
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			HTable table = new HTable(conf,"flux");
			
			List<FluxInfo> retuList = new ArrayList<>();
			Scan scan = new Scan();
			scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(reg)));
			ResultScanner rs = table.getScanner(scan);
			Result r = null;
			while((r = rs.next()) != null){
				String time = new String(r.getValue("cf1".getBytes(), "time".getBytes()));
				String uv_id = new String(r.getValue("cf1".getBytes(), "uv_id".getBytes()));
				String ss_id = new String(r.getValue("cf1".getBytes(), "ss_id".getBytes()));
				String ss_time = new String(r.getValue("cf1".getBytes(), "ss_time".getBytes()));
				String urlname = new String(r.getValue("cf1".getBytes(), "urlname".getBytes()));
				String cip = new String(r.getValue("cf1".getBytes(), "cip".getBytes()));
				FluxInfo fi = new FluxInfo(time, uv_id, ss_id, ss_time, urlname, cip);
				retuList.add(fi);
			}
			
			return retuList;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	

	public static void insertData(FluxInfo fi){
		try {
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum","hadoop01:2181,hadoop02:2181,hadoop03:2181");
			
			HTable table = new HTable(conf,"flux");
			Put put = new Put(Bytes.toBytes(fi.getTime()+"_"+fi.getUv_id()+"_"+fi.getSs_id()+"_"+fi.getCip()+"_"+FluxUtils.randNum(8)));
			put.add(Bytes.toBytes("cf1"),Bytes.toBytes("time"),Bytes.toBytes(fi.getTime()));
			put.add(Bytes.toBytes("cf1"),Bytes.toBytes("uv_id"),Bytes.toBytes(fi.getUv_id()));
			put.add(Bytes.toBytes("cf1"),Bytes.toBytes("ss_id"),Bytes.toBytes(fi.getSs_id()));
			put.add(Bytes.toBytes("cf1"),Bytes.toBytes("ss_time"),Bytes.toBytes(fi.getSs_time()));
			put.add(Bytes.toBytes("cf1"),Bytes.toBytes("urlname"),Bytes.toBytes(fi.getUrlname()));
			put.add(Bytes.toBytes("cf1"),Bytes.toBytes("cip"),Bytes.toBytes(fi.getCip()));
			table.put(put);
			
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}

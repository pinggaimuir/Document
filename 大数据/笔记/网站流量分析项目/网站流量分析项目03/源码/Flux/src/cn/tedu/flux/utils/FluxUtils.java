package cn.tedu.flux.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FluxUtils {
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
	
	private FluxUtils() {
	}
	
	public static String formatDate(Date date){
		return format.format(date);
	}
	
	public static Date parseDateStr(String dateStr){
		try {
			return format.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public static long randNum(int len){
		double num = Math.random();
		return Math.round(num * Math.pow(10, len));
	}
}

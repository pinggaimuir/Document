package cn.tedu.xx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DDD {
	public static void main(String[] args) {
		String pre = "^(?:[^\\|]*\\|){15}(?:[^\\|]*_[^\\|]*_([^\\|]*)\\|)[^\\|]*$";
		String str = "9999|http://hadoop01/demo/a.jsp|a.jsp|页面A|utf-8|1536x864|32-bit|zh-cn|1|1|10.0|0.8854856940742033||Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 10.0; WOW64; Trident/8.0; Touch; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; Tablet PC 2.0)|30124168496535978032|7622515780_0_1480734576|192.168.242.1";

		System.out.println(str.matches(pre));

		Pattern p = Pattern.compile(pre);
		Matcher matcher = p.matcher(str);

		//查找匹配的子串
		matcher.find();
		int count = matcher.groupCount();
		System.out.println(count);
		for(int i = 0;i<=count;i++){
			String s = matcher.group(i);
			System.out.println(s);
		}
	}
}

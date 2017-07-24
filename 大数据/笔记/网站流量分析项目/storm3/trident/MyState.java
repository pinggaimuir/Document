package cn.tedu.trident;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import storm.trident.state.State;

public class MyState implements State {

	private Map<String ,Integer> map = new ConcurrentHashMap<>();
	
	public void put(String str,Integer num){
		map.put(str, num);
	}
	
	public int get(String str){
		return map.containsKey(str) ? map.get(str) : 0;
	}
	
	@Override
	public void beginCommit(Long txid) {
		
	}

	@Override
	public void commit(Long txid) {
		
	}

}

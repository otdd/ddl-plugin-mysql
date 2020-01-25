package io.otdd.ddl.mysql.common;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class SettingsUtil {
	
	private static MysqlRespType currentRespType = MysqlRespType.RESP_TYPE_RESULTSET;
	
	private static String RESP_TYPE = "response type";
	
	//do not support decode request currently.
	public static Map<String, String> getReqProtocolSettings() {
		return null;
	}
	
	public static Map<String, String> getRespProtocolSettings() {
		JSONObject json = new JSONObject();
		json.put("hint", "the mysql response type.");
		json.put("type", "select");
		json.put("currentValue", currentRespType.toString());
		JSONArray jsonArray = new JSONArray();
		jsonArray.add(MysqlRespType.RESP_TYPE_RESULTSET.toString());
		jsonArray.add(MysqlRespType.RESP_TYPE_PREPARED_STATEMENT.toString());
		jsonArray.add(MysqlRespType.RESP_TYPE_OK.toString());
		json.put("values", jsonArray);
		Map<String,String> ret = new HashMap<String,String>();
		ret.put(RESP_TYPE,json.toString());
		
		return ret;
	}
	
	public static void setDefaultSettings(Map<String, String> reqSettings,Map<String, String> respSettings){
		if(respSettings!=null){
			String typeDesc = respSettings.get(RESP_TYPE);
			MysqlRespType tmp = descToType(typeDesc);
			if(tmp!=null){
				currentRespType = tmp;
			}
		}
	}
	
	public static MysqlRespType getRespType(Map<String, String> settings) {
		if(settings!=null){
			String typeDesc = settings.get(RESP_TYPE);
			MysqlRespType tmp = descToType(typeDesc);
			if(tmp!=null){
				return tmp;
			}
		}
		return currentRespType;
	}
	
	private static MysqlRespType descToType(String desc){
		if(MysqlRespType.RESP_TYPE_RESULTSET.equalsDesc(desc)){
			return MysqlRespType.RESP_TYPE_RESULTSET;
		}
		else if(MysqlRespType.RESP_TYPE_PREPARED_STATEMENT.equalsDesc(desc)){
			return MysqlRespType.RESP_TYPE_PREPARED_STATEMENT;
		}
		else if(MysqlRespType.RESP_TYPE_OK.equalsDesc(desc)){
			return MysqlRespType.RESP_TYPE_OK;
		}
		return null;
	}

}

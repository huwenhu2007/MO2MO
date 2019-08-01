package oplog.listener.mongo;

import com.alibaba.fastjson.JSONObject;
import utils.Utilitys;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TargetMongoConfig {

	   
	private List<Map<String,String>> strDBNameList=new ArrayList<Map<String,String>>();

	public void addAuthInfo(String strDBName, String strUserName, String strPassword){
	   if(Utilitys.isNil(strDBName)||Utilitys.isNil(strUserName)||Utilitys.isNil(strPassword)){
		   return;
	   }
		Map<String,String> authMap=new HashMap<String,String>();
		authMap.put("strDBName", strDBName);
		authMap.put("strUserName", strUserName);
		authMap.put("strPassword", strPassword);
	   this.strDBNameList.add(authMap);
	}


	private String strIP;
	private int nPort;
	private int nVersion;
	private boolean isDebug;

	public String getStrIP() {
		return strIP;
	}

	public void setStrIP(String strIP) {
		this.strIP = strIP;
	}

	public int getnPort() {
		return nPort;
	}

	public void setnPort(int nPort) {
		this.nPort = nPort;
	}

	public int getnVersion() {
		return nVersion;
	}

	public void setnVersion(int nVersion) {
		this.nVersion = nVersion;
	}

	public List<Map<String,String>> getStrDBNameList() {
		return strDBNameList;
	}

	public boolean isDebug(){
		return isDebug;
	}

	public TargetMongoConfig(String strIP, int nPort, int nVersion, boolean isDebug) {
		this.strIP = strIP;
		this.nPort = nPort;
		this.nVersion = nVersion;
		this.isDebug = isDebug;
	}


	/**
	 * 数据库转换规则
	 */
	private JSONObject jsonDBRule;
	/**
	 * 表转换规则
	 */
	private JSONObject jsonTableRule;
	/**
	 * 修改失败是否进行插入操作
	 */
	private int nUpdateFailToInsert;

	/**
	 * 设置规则信息
	 * @param jsonDBRule		库配置信息
	 * @param jsonTableRule	表配置信息
	 */
	public void setRule(JSONObject jsonDBRule, JSONObject jsonTableRule, int nUpdateFailToInsert){
		this.jsonDBRule = jsonDBRule;
		this.jsonTableRule = jsonTableRule;
		this.nUpdateFailToInsert = nUpdateFailToInsert;
	}

	public JSONObject getJsonDBRule() {
		return jsonDBRule;
	}

	public JSONObject getJsonTableRule() {
		return jsonTableRule;
	}

	public int getnUpdateFailToInsert() {
		return nUpdateFailToInsert;
	}

	public String toString(){
		 StringBuilder str=new StringBuilder();
		 str.append("strIP=").append(strIP).append(" nPort=").append(nPort).append(" nVersion=").append(nVersion).append("\n").append("[");
		 for (int i = 0; i < strDBNameList.size(); i++) {
			  Map<String,String> authMap=strDBNameList.get(i);
			  str.append(" strDBName=").append(authMap.get("strDBName")).append(" strUserName=").append(authMap.get("strUserName")).append(",");
		 }
		 
		str.append("]").append("\n");
		str.append("jsonDBObject").append(":").append(jsonDBRule == null ? null : jsonDBRule.toString()).append("\n");
		str.append("jsonTableObject").append(":").append(jsonTableRule == null ? null : jsonTableRule.toString()).append("\n");
		return str.toString();
	}
 
	   
}

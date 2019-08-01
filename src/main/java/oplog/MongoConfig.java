package oplog;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class MongoConfig {
	private String strWorkerId;
	private String strIP;
	private int nPort;
	private String strUserName;
	private String strPassword;
	private String strOplogModel;
	private int nVersion;
	private int nPositionEnable;
	private int nTime;
	private int nIncrement;
	private int nDebug;
	
	private String strEndLogPosPath;
	private JSONArray strDMLTargetJSONArray;
	private JSONArray arrOplogEventFilter;
	private JSONArray arrOplogDataFilter;

	public MongoConfig(JSONObject jsonObject){
		this.strWorkerId = jsonObject.getString("strWorkerId");
		this.strIP = jsonObject.getString("strDBIP");
		this.nPort = jsonObject.getIntValue("nDBPort");
		this.strUserName = jsonObject.getString("strUserName");
		this.strPassword = jsonObject.getString("strPassWord");
		this.strOplogModel = jsonObject.getString("strOplogModel");
		this.nVersion = jsonObject.getIntValue("nVersion");
		this.nDebug = jsonObject.getIntValue("nDebug");
		this.nTime = jsonObject.getIntValue("nTime");
		this.nIncrement = jsonObject.getIntValue("nIncrement");
		this.nPositionEnable = jsonObject.getIntValue("nPositionEnable");
		this.strEndLogPosPath = jsonObject.getString("strEndLogPosPath");
		this.strDMLTargetJSONArray = jsonObject.getJSONArray("strDMLTargetJSONArray");
		this.arrOplogEventFilter = jsonObject.getJSONArray("arrOplogEventFilter");
		this.arrOplogDataFilter = jsonObject.getJSONArray("arrOplogDataFilter");
	}
	
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
	public String getStrUserName() {
		return strUserName;
	}
	public void setStrUserName(String strUserName) {
		this.strUserName = strUserName;
	}
	public String getStrPassword() {
		return strPassword;
	}
	public void setStrPassword(String strPassword) {
		this.strPassword = strPassword;
	}
	public int getnIncrement() {
		return nIncrement;
	}

	public void setnIncrement(int nIncrement) {
		this.nIncrement = nIncrement;
	}

	public int getnTime() {
		return nTime;
	}

	public void setnTime(int nTime) {
		this.nTime = nTime;
	}

	public String getStrEndLogPosPath() {
		return strEndLogPosPath;
	}

	public void setStrEndLogPosPath(String strEndLogPosPath) {
		this.strEndLogPosPath = strEndLogPosPath;
	}

	public String getStrWorkerId() {
		return strWorkerId;
	}

	public void setStrWorkerId(String strWorkerId) {
		this.strWorkerId = strWorkerId;
	}

	public JSONArray getStrDMLTargetJSONArray() {
		return strDMLTargetJSONArray;
	}

	public void setStrDMLTargetJSONArray(JSONArray strDMLTargetJSONArray) {
		this.strDMLTargetJSONArray = strDMLTargetJSONArray;
	}

	public boolean isDebug(){
		return nDebug == 1 ? true : false;
	}


	public void setnDebug(int nDebug) {
		this.nDebug = nDebug;
	}

	public int getnVersion() {
		return nVersion;
	}

	public void setnVersion(int nVersion) {
		this.nVersion = nVersion;
	}

	public int getnPositionEnable() {
		return nPositionEnable;
	}

	public void setnPositionEnable(int nPositionEnable) {
		this.nPositionEnable = nPositionEnable;
	}

	public String getStrOplogModel() {
		return strOplogModel;
	}

	public void setStrOplogModel(String strOplogModel) {
		this.strOplogModel = strOplogModel;
	}

	public JSONArray getArrOplogEventFilter() {
		return arrOplogEventFilter;
	}

	public void setArrOplogEventFilter(JSONArray arrOplogEventFilter) {
		this.arrOplogEventFilter = arrOplogEventFilter;
	}

	public JSONArray getArrOplogDataFilter() {
		return arrOplogDataFilter;
	}

	public void setArrOplogDataFilter(JSONArray arrOplogDataFilter) {
		this.arrOplogDataFilter = arrOplogDataFilter;
	}

	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(strIP).append("|");
		sb.append(nPort).append("|");
		sb.append(strWorkerId);
		return sb.toString();
	}
}

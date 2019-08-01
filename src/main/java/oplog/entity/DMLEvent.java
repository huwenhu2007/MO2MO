package oplog.entity;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;

public class DMLEvent {
	public final static String INSERT = "i";

	public final static String UPDATE = "u";

	public final static String DELETE = "d";

	private String strKey = "";

	private String strCollectionName;

	private String strDBName;

	private BasicDBObject queryObj;
	
	private BasicDBObject updObj;

	private String strActionType;

	public String getStrCollectionName() {
		return strCollectionName;
	}

	public void setStrCollectionName(String strCollectionName) {
		this.strCollectionName = strCollectionName;
	}

	public String getStrDBName() {
		return strDBName;
	}

	public void setStrDBName(String strDBName) {
		this.strDBName = strDBName;
	}
	
	public BasicDBObject getQueryObj() {
		return queryObj;
	}

	public void setQueryObj(BasicDBObject queryObj) {
		this.queryObj = queryObj;
	}

	public BasicDBObject getUpdObj() {
		return updObj;
	}

	public void setUpdObj(BasicDBObject updObj) {
		this.updObj = updObj;
	}

	public String getStrActionType() {
		return strActionType;
	}

	public void setStrActionType(String strActionType) {
		this.strActionType = strActionType;
	}

	public String getStrKey() {
		return strKey;
	}

	public void setStrKey(String strKey) {
		this.strKey = strKey;
	}


	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("strDBName:").append(strDBName).append(";")
			.append("strCollectionName:").append(strCollectionName).append(";")
			.append("strActionType:").append(strActionType).append(";")
			.append("queryObj:").append(queryObj == null ? null : queryObj.toString()).append(";")
			.append("updObj:").append(updObj == null ? null : updObj.toString()).append(";");
		return sb.toString();
	}

	public String toJSONString(){
		JSONObject jsonObject = new JSONObject();
		jsonObject.put("db", strDBName);
		jsonObject.put("collection", strCollectionName);
		jsonObject.put("op", strActionType);
		jsonObject.put("q", queryObj);
		jsonObject.put("u", updObj);
		return jsonObject.toJSONString();
	}

}

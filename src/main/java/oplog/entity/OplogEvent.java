package oplog.entity;

import com.mongodb.DBObject;

public class OplogEvent {
	
	private String strIP;
	private int nPort;
	private String strWorkId;
	private DBObject dbObject;

	public OplogEvent(){
	}
	
	public OplogEvent(String strIP, int nPort, String strWorkId, DBObject dbObject){
		this.strIP = strIP;
		this.nPort = nPort;
		this.strWorkId = strWorkId;
		this.dbObject = dbObject;
	}

	public void from(OplogEvent oplogEvent){
		this.strIP = oplogEvent.getStrIP();
		this.nPort = oplogEvent.getnPort();
		this.strWorkId = oplogEvent.getStrWorkId();
		this.dbObject = oplogEvent.getDbObject();
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

	public String getStrWorkId() {
		return strWorkId;
	}

	public void setStrWorkId(String strWorkId) {
		this.strWorkId = strWorkId;
	}

	public DBObject getDbObject() {
		return dbObject;
	}

	public void setDbObject(DBObject dbObject) {
		this.dbObject = dbObject;
	}

	public String toString(){
		return dbObject.toString();
	}

}

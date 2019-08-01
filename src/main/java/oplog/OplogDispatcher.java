package oplog;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import oplog.entity.DMLEvent;
import oplog.listener.DMLListener;
import org.apache.log4j.Logger;

/**
 * 目标操作信息转发
 */
public class OplogDispatcher {
	
	private Logger logger = Logger.getLogger(OplogDispatcher.class);

	/**
	 * 数据源mongo配置信息
	 */
	private String strSourceSign;
	/**
	 * 目标接口
	 */
	private DMLListener dmlListener;
	/**
	 * 上次成功转发的对象信息
	 */
	private DBObject successDBObject;

	public OplogDispatcher(String strSourceSign) {
		this.strSourceSign = strSourceSign;
	}

	public DMLListener getDMLListener() {
		return dmlListener;
	}

	public void setDMLListener(DMLListener dmlListener) {
		this.dmlListener = dmlListener;
	}

	/**
	 * 事件转发
	 * @param dbObject
	 * @throws Exception
	 */
	public void dispatch(DBObject dbObject)
			throws Exception {
		if (dbObject == null) {
			logger.info(strSourceSign+" target dbObject is null");
			return;
		}
		// 事件转换（只处理增加、修改、删除事件）
		String eventType = String.valueOf(dbObject.get("op"));
		DMLEvent eventData = null;
		if (DMLEvent.DELETE.equals(eventType)) {
			eventData = buildDeleteEvent(dbObject);
		} else if (DMLEvent.INSERT.equals(eventType)) {
			eventData = buildInertEvent(dbObject);
		} else if (DMLEvent.UPDATE.equals(eventType)) {
			eventData = buildUpdateEvent(dbObject);
		} else {
			logger.info(strSourceSign+" eventType ----> "+ eventType);
		}
		if (eventData != null) {
			dmlListener.onEvent(eventData);
			// 设置上次成功转发的对象
			successDBObject = dbObject;
		}
	}

	private DMLEvent buildInertEvent(DBObject dbObject) {
		String strDB_Collection = String.valueOf(dbObject.get("ns"));
		String[] arrDB_Collection = strDB_Collection.split("\\.");
		String strDB = arrDB_Collection[0];
		String strCollection = arrDB_Collection[1];
		BasicDBObject insObject = (BasicDBObject)dbObject.get("o");
		
		DMLEvent eventData = new DMLEvent();
		eventData.setStrActionType(DMLEvent.INSERT);
		eventData.setStrDBName(strDB);
		eventData.setStrCollectionName(strCollection);
		eventData.setUpdObj(insObject);
		return eventData;
	}

	private DMLEvent buildUpdateEvent(DBObject dbObject) {
		String strDB_Collection = String.valueOf(dbObject.get("ns"));
		String[] arrDB_Collection = strDB_Collection.split("\\.");
		String strDB = arrDB_Collection[0];
		String strCollection = arrDB_Collection[1];
		BasicDBObject queryObject = (BasicDBObject)dbObject.get("o2");
		BasicDBObject updObject = (BasicDBObject)dbObject.get("o");
		
		DMLEvent eventData = new DMLEvent();
		eventData.setStrActionType(DMLEvent.UPDATE);
		eventData.setStrDBName(strDB);
		eventData.setStrCollectionName(strCollection);
		eventData.setQueryObj(queryObject);
		eventData.setUpdObj(updObject);
		return eventData;
	}

	private DMLEvent buildDeleteEvent(DBObject dbObject) {
		String strDB_Collection = String.valueOf(dbObject.get("ns"));
		String[] arrDB_Collection = strDB_Collection.split("\\.");
		String strDB = arrDB_Collection[0];
		String strCollection = arrDB_Collection[1];
		BasicDBObject bsonObject = (BasicDBObject)dbObject.get("o");
		
		DMLEvent eventData = new DMLEvent();
		eventData.setStrActionType(DMLEvent.DELETE);
		eventData.setStrDBName(strDB);
		eventData.setStrCollectionName(strCollection);
		eventData.setQueryObj(bsonObject);
		return eventData;
	}

	/**
	 * 获取上次成功转发的对象信息
	 * @return
	 */
	public DBObject getSuccessDBObject() {
		return successDBObject;
	}
}

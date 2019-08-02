package oplog.listener.mongo.impl;


import com.alibaba.fastjson.JSONObject;
import com.mongodb.*;
import oplog.entity.DMLEvent;
import oplog.listener.DMLListener;
import oplog.listener.DMLListenerAbs;
import oplog.listener.mongo.TargetMongoConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DMLMongoListener extends DMLListenerAbs {

	private Logger logger = Logger.getLogger(DMLMongoListener.class);

	private TargetMongoConfig targetMongoConfig;

	private MongoClient mongoClient;

	private String strSourceSign;

	public DMLMongoListener() {
		
 	}

	/**
	 * 初始化配置信息
	 * @param strWorkSign	 任务标记
	 * @param strSign		 消费者编号
	 * @param jsonObject	 配置信息
	 * @param isDebug        debug模式
	 * @throws Exception
	 */
	public void init(String strWorkSign, String strSign, JSONObject jsonObject, boolean isDebug) throws Exception {
		strSourceSign = new StringBuilder().append(strWorkSign).append("-").append(strSign).toString();
		logger.info(String.format("%s target mongo init start", strSourceSign));

		if(jsonObject == null || jsonObject.isEmpty()){
			throw new RuntimeException(String.format("%s 目标配置信息不存在", strSourceSign));
		}
		// 实例化目标库配置对象
		String strMongoIP = jsonObject.getString("strMongoIP");
		int nMongoPort = jsonObject.getIntValue("nMongoPort");
		int nVersion = jsonObject.getIntValue("nVersion");
		targetMongoConfig = new TargetMongoConfig(strMongoIP,nMongoPort,nVersion,isDebug);

		String strMongoDBName = jsonObject.getString("strMongoDBName");
		String strMongoUserName = jsonObject.getString("strMongoUserName");
		String strMongoPassWord = jsonObject.getString("strMongoPassWord");
		targetMongoConfig.addAuthInfo(strMongoDBName,strMongoUserName,strMongoPassWord);

		JSONObject jsonDBObject = jsonObject.getJSONObject("jsonDBRule");
		JSONObject jsonTableObject = jsonObject.getJSONObject("jsonTableRule");
		int nUpdateFailToInsert = jsonObject.getIntValue("nUpdateFailToInsert");
		targetMongoConfig.setRule(jsonDBObject, jsonTableObject, nUpdateFailToInsert);

		logger.info(String.format("%s target mongo init end%n%s", strSourceSign, targetMongoConfig.toString()));
	}

	/**
	 * 启动目标任务，连接目标库
	 * @throws Exception
	 */
	public void start() throws Exception {
		ServerAddress sa  = new ServerAddress(targetMongoConfig.getStrIP(),targetMongoConfig.getnPort());
		List<MongoCredential> mongoCredentialList = new ArrayList<MongoCredential>();
		for (int i = 0; i < targetMongoConfig.getStrDBNameList().size(); i++) {
			Map<String,String> authMap=targetMongoConfig.getStrDBNameList().get(i);
			MongoCredential mongoCredential = null;
			if(targetMongoConfig.getnVersion() == 2){
				// 支持mongo2.0
				mongoCredential =	MongoCredential.createMongoCRCredential(authMap.get("strUserName"),authMap.get("strDBName"),authMap.get("strPassword").toCharArray());
			} else {
				// 支持mongo3.0
				mongoCredential =	MongoCredential.createScramSha1Credential(authMap.get("strUserName"),authMap.get("strDBName"),authMap.get("strPassword").toCharArray());
			}
			mongoCredentialList.add(mongoCredential);
		}
		mongoClient=new MongoClient(sa,mongoCredentialList);
	}

	/**
	 * 执行目标任务
	 * @param event
	 */
	public void onEvent(DMLEvent event) throws Exception{
		if(targetMongoConfig.isDebug()){
			logger.error(String.format("%s 目标处理事件信息:%s", strSourceSign, event.toString()));
		}

		// 获取事件中的库表信息
		JSONObject jsonObject = ruleChangeName(event, targetMongoConfig.getJsonDBRule(), targetMongoConfig.getJsonTableRule());
		if(jsonObject.isEmpty()){
			logger.error(String.format("%s 事件中不存在库表信息", strSourceSign));
			return;
		}

		// 获取需要操作的库表名称
		String strDBName = jsonObject.getString("strDBName");
		String strCollectionName = jsonObject.getString("strCollectionName");
		@SuppressWarnings("deprecation")
		DB db=mongoClient.getDB(strDBName);
		DBCollection coll = db.getCollection(strCollectionName);
		// 返回默认值
		int result=-1;
		// 按照事件调用对应的方法
		String strTransfer=event.getStrActionType();
		if("i".equals(strTransfer)){
			result=insert(event,coll);
		}else if("u".equals(strTransfer)){
			result=update(event,coll);
		}else if("d".equals(strTransfer)){
			result=delete(event,coll);
		}else{
			logger.info(String.format("%s 未找到事件的处理方法 %s",strSourceSign,event.toString()));
		}
		if(result==0){
    		throw new RuntimeException(String.format("%s 目标事件处理失败 result:%d",event.toString(),result));
		}
	}

	/**
	 * 断开目标库连接
	 */
	public void destroy() {
		if(this.mongoClient!=null){
			this.mongoClient.close();
		}
	}

	/**
	 * 获取插入数据id
	 * @param insertBsonObject
	 * @return
	 */
	private String getId(BasicDBObject insertBsonObject){
		return insertBsonObject.toString();
	}

	/**
	 * 改变update事件为insert事件
	 * @param event
	 */
	private void changeUpdateEvent2InsertEvent(DMLEvent event){
		// 修改对象
		BasicDBObject updateBsonObject = event.getUpdObj();
		if(updateBsonObject.containsKey("$set")){
			BSONObject u = (BSONObject)updateBsonObject.get("$set");
			updateBsonObject.remove("$set");
			updateBsonObject.putAll(u);
		}

		// 查询对象
		BasicDBObject queryBsonObject = event.getQueryObj();
		// 获取主键id
		Object obj = queryBsonObject.get("_id");
		updateBsonObject.put("_id", obj);
		// 清理查询对象
		event.setQueryObj(null);
	}

	/**
	 * 插入数据
	 * @param event
	 * @param coll
	 * @return
	 */
	private int insert(DMLEvent event,DBCollection coll) throws Exception{
		// 获取操作插入数据对象
		BasicDBObject insertBsonObject = event.getUpdObj();

		// 判断对象是否正常
		if(insertBsonObject == null){
			logger.info(String.format("%s %s 事件对象updObj is null",strSourceSign,event.toString()));
			return  -1;
		}
		if(insertBsonObject.toMap().size() == 0){
			logger.info(String.format("%s %s 事件对象updObj is empty",strSourceSign,event.toString()));
			return  -1;
		}
		// 判断记录是否已经存在
		DBObject existResult= coll.findOne(insertBsonObject) ;
		if(existResult!=null&&existResult.keySet().size()>0){
			if(targetMongoConfig.isDebug()) {
		 		logger.info(String.format("%s %s 已存在", strSourceSign, event.toString()));
			}
		 	return -1;
		}

		// 保存数据
		long lStartTime= System.currentTimeMillis();
		coll.save(insertBsonObject);
		long lEndTime= System.currentTimeMillis();
		if((lEndTime - lStartTime)>1000){
			if(targetMongoConfig.isDebug()) {
				logger.info(String.format("%s %s insert time is %d", strSourceSign, event.toString(), (lEndTime - lStartTime)));
			}
		}
		return 1;
	}

	/**
	 * 修改数据
	 * @param event
	 * @param coll
	 * @return
	 */
	private int update(DMLEvent event,DBCollection coll) throws Exception{
		// 查询对象
		BasicDBObject queryBsonObject = event.getQueryObj();
		// 修改对象
		BasicDBObject updateBsonObject = event.getUpdObj();
		// 判断查询对象是否正常
		if(queryBsonObject == null){
			logger.info(String.format("%s %s 查询对象queryObj is null",strSourceSign,event.toString()));
			return  -1;
		}
		if(queryBsonObject.toMap().size() == 0){
			logger.info(String.format("%s %s 查询对象queryObj is empty",strSourceSign,event.toString()));
			return  -1;
		}

		// 判断修改对象是否正常
		if(updateBsonObject == null){
			logger.info(String.format("%s %s 事件对象updObj is null",strSourceSign,event.toString()));
			return  -1;
		}
		if(updateBsonObject.toMap().size() == 0){
			logger.info(String.format("%s %s 事件对象updObj is empty",strSourceSign,event.toString()));
			return  -1;
		}

		// 进行修改操作
		long lStartTime= System.currentTimeMillis();
		WriteResult result= coll.update(queryBsonObject, updateBsonObject, false, true);
		long lEndTime= System.currentTimeMillis();
		if((lEndTime - lStartTime)>1000){
			if(targetMongoConfig.isDebug()) {
				logger.info(String.format("%s %s update time is %d", strSourceSign, event.toString(), (lEndTime - lStartTime)));
			}
		}
		// 判断修改结果
		int nReturn = result.getN();
		if(nReturn==0){
			if(targetMongoConfig.isDebug()) {
				logger.info(String.format("%s %s update result is %d", strSourceSign, event.toString(), nReturn));
			}
			if(targetMongoConfig.getnUpdateFailToInsert() == 1) {
				// 修改失败进行插入操作
				changeUpdateEvent2InsertEvent(event);
				if (targetMongoConfig.isDebug()) {
					logger.info(String.format("%s 转换为新增之后的信息 %s", strSourceSign, event.toString()));
				}
				return insert(event, coll);
			}
		}
		return -1;
	}

	/**
	 * 删除操作
	 * @param event
	 * @param coll
	 * @return
	 */
	private int delete(DMLEvent event,DBCollection coll) throws Exception {
		// 获取查询对象
		BasicDBObject queryBsonObject = event.getQueryObj();

		// 判断查询对象是否正常
		if(queryBsonObject == null){
			logger.info(String.format("%s %s 查询对象queryObj is null",strSourceSign,event.toString()));
			return  -1;
		}
		if(queryBsonObject.toMap().size() == 0){
			logger.info(String.format("%s %s 查询对象queryObj is empty",strSourceSign,event.toString()));
			return  -1;
		}
		// 进行删除操作
		long lStartTime= System.currentTimeMillis();
		coll.remove(queryBsonObject);
		long lEndTime= System.currentTimeMillis();
		if((lEndTime - lStartTime)>1000){
			if(targetMongoConfig.isDebug()) {
				logger.info(String.format("%s %s delete time is %d", strSourceSign, event.toString(), (lEndTime - lStartTime)));
			}
		}

		return -1;
	}

    public static void main(String[] args) {
    	 
			
	}

}

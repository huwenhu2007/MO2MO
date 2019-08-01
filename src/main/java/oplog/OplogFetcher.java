package oplog;

import com.alibaba.fastjson.JSONArray;
import com.mongodb.*;
import oplog.entity.OplogPosition;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * mongo日志抓取对账
 */
public class OplogFetcher{

	private static Logger logger = Logger.getLogger(OplogFetcher.class);
	
	private MongoConfig mongoConfig;

	public OplogFetcher(MongoConfig mongoConfig){
		this.mongoConfig = mongoConfig;
	}
	
	private MongoClient mongoClient;
	private DBCursor opCursor;

	/**
	 * 获取客户端对账
	 * @throws Exception
	 */
	private void initMongoClient() throws Exception {
		// 获取mongo服务地址信息
		ServerAddress sa  = new ServerAddress(mongoConfig.getStrIP(), mongoConfig.getnPort());
		// 获取mongo服务认证集合
		List<MongoCredential> mongoCredentialList = new ArrayList<MongoCredential>();
		MongoCredential mongoCredential = null;
		if(mongoConfig.getnVersion() == 2){
			// 支持mongo2.0
			mongoCredential =	MongoCredential.createMongoCRCredential(mongoConfig.getStrUserName(),"admin",mongoConfig.getStrPassword().toCharArray());
		} else {
			// 支持mongo3.0
			mongoCredential =	MongoCredential.createScramSha1Credential(mongoConfig.getStrUserName(),"admin",mongoConfig.getStrPassword().toCharArray());
		}
		mongoCredentialList.add(mongoCredential);
		// 链接Mongo服务
		mongoClient=new MongoClient(sa, mongoCredentialList);
	}
	
	public void initDBCursor(OplogPosition oplogPosition) throws Exception {
		// 链接mongo服务
		initMongoClient();
		// 获取日志集合
		DBCollection fromCollection = null;
		if("rs".equals(mongoConfig.getStrOplogModel())){
			fromCollection = mongoClient.getDB("local").getCollection("oplog.rs");
		} else {
			fromCollection = mongoClient.getDB("local").getCollection("oplog.$main");
		}
		// 设置日志起始时间
		DBObject timeQuery = new BasicDBObject(
	            "ts", new BasicDBObject("$gt", new BSONTimestamp(oplogPosition.getnTime(), oplogPosition.getnIncrement())));
		// 添加事件过滤条件
		JSONArray arrOplogEventFilter = mongoConfig.getArrOplogEventFilter();
		DBObject event_Query = new BasicDBObject("$in", arrOplogEventFilter);
		timeQuery.put("op", event_Query);
		// 获取集合过滤条件
		JSONArray arrOplogDataFilter = mongoConfig.getArrOplogDataFilter();
		int nLength = arrOplogDataFilter.size();
		Pattern[] arrPattern = new Pattern[nLength];
		for(int i = 0;i < nLength;i++){
			String strOplogDataFilter = arrOplogDataFilter.getString(i);
			Pattern pattern = Pattern.compile(strOplogDataFilter);
			arrPattern[i] = pattern;
		}
		DBObject slaveObject = new BasicDBObject("$in", arrPattern);
		timeQuery.put("ns", slaveObject);

		logger.info(String.format("%s 查询条件：%s",mongoConfig.toString(),timeQuery.toString()));
		// 获取mongo查询指针
		opCursor = fromCollection.find(timeQuery)
											.sort(new BasicDBObject("$natural", 1))
											.addOption(Bytes.QUERYOPTION_OPLOGREPLAY)	// 提高带ts的语句查询速度
								            .addOption(Bytes.QUERYOPTION_TAILABLE)		 
								            .addOption(Bytes.QUERYOPTION_AWAITDATA)		
								            .addOption(Bytes.QUERYOPTION_NOTIMEOUT);
	}

	/**
	 * 关闭Mongo
	 */
	public void closeMongo(){
		if(opCursor != null){
			opCursor.close();
		}
		if(mongoClient != null){
			mongoClient.close();
		}
	}

	/**
	 * 获取当前查询指针
	 * @return
	 */
	public DBCursor getDBCursor(){
		return opCursor;
	}


	public static void main(String[] args) {
	}
}

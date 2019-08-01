package oplog.task;

import com.mongodb.DBObject;
import oplog.MongoConfig;
import oplog.OplogFetcher;
import oplog.entity.OplogEvent;
import oplog.entity.OplogPosition;
import oplog.queue.OplogEventRingBuffer;
import org.apache.log4j.Logger;
import utils.PositionUtils;

/**
 * oplog日志抓取线程
 */
public class OplogFetcherTask {
	
	private Logger logger = Logger.getLogger(OplogFetcherTask.class);
	/**
	 * 任务配置信息
	 */
	private MongoConfig mongoConfig;
	/**
	 * 与Mongo交互对象
	 */
	private OplogFetcher oplogFetcher;
	/**
	 * 事件队列
	 */
	private OplogEventRingBuffer oplogEventRingBuffer;
	/**
	 * 运行状态
	 */
	private boolean isRuning=false;
	/**
	 * 抓取起始位置信息
	 */
	private OplogPosition oplogPosition;
	/**
	 * 异常信息
	 */
	private String strExceptionMessage;

	public OplogFetcherTask(MongoConfig mongoConfig, OplogEventRingBuffer oplogEventRingBuffer) throws Exception {
		this.mongoConfig = mongoConfig;
		this.oplogEventRingBuffer = oplogEventRingBuffer;
		// 创建oplog抓取对象（与mongo交互对象）
		oplogFetcher = new OplogFetcher(this.mongoConfig);
	}

	/**
	 * 开启抓取任务
	 * @throws Exception
	 */
	 public  void startFetch() throws Exception {
		 logger.info(String.format("%s抓取线程 starting",this.mongoConfig.toString()));
		 // 获取oplog日志位置信息
		 oplogPosition = PositionUtils.findBinlogPositionList(this.mongoConfig);
		 logger.info(String.format("%s oplog is %s",this.mongoConfig.toString(),oplogPosition.toString()));
		 // 初始化查询指针对象
		 oplogFetcher.initDBCursor(oplogPosition);
		 // 启动抓取线程
		 new OplogFetcherThread().start();
		 logger.info(String.format("%s抓取线程 started",this.mongoConfig.toString()));
	 }

	/**
	 * oplog日志抓取线程
	 */
	class OplogFetcherThread extends Thread {

		 @Override
		 public void run() {
			 isRuning = true;
			 setExceptionMessage("");
			 try {
				 // 线程运行且mongo指针不为null
				 while (isRuning && oplogFetcher.getDBCursor() != null) {
					 if (!oplogFetcher.getDBCursor().hasNext()) {
						 // 无数据则休眠200毫秒，防止对cpu资源大量消耗
						 Thread.sleep(200);
						 continue;
					 } else {
						 // 获取查询数据
						 DBObject nextOp = oplogFetcher.getDBCursor().next();
						 // 生成事件对象
						 OplogEvent oplogEvent = new OplogEvent(mongoConfig.getStrIP(), mongoConfig.getnPort(), mongoConfig.getStrWorkerId(), nextOp);
						 // debug模式
						 if(mongoConfig.isDebug()){
							logger.info(String.format("%s日志内容：%s",mongoConfig.toString(),oplogEvent.toString()));
						 }
						 // 将事件对象放入队列
						 oplogEventRingBuffer.publishData(oplogEvent);
					 }
				 }
			 } catch (Exception e) {
				 logger.error(mongoConfig.toString() + " 抓取线程 error " + e.getMessage(), e);
				 setExceptionMessage(e.getMessage());
				 // 抓取任务异常停止
				 stopFetch();
				 return;
			 }
			 logger.info(mongoConfig.toString() + " 抓取线程 stop ....");
			 // 抓取任务正常停止
			 stopFetch();
		 }
	 }

	/**
	 * 获取抓取线程运行状态
	 * @return
	 */
	public boolean isRuning() {
		return isRuning;
	}

	/**
	 * 停止抓取任务
	 */
	public void stopFetch(){
		if(oplogFetcher != null){
			oplogFetcher.closeMongo();
		}
		isRuning = false;
	}

	/**
	 * 获取抓取起始位置信息
	 * @return
	 */
	public OplogPosition getOplogPosition() {
		return oplogPosition;
	}

	/**
	 * 获取异常信息
	 * @return
	 */
	public String getExceptionMessage() {
		return strExceptionMessage == null ? "" : strExceptionMessage;
	}

	/**
	 * 修改异常信息
	 * @param strExceptionMessage
	 */
	public void setExceptionMessage(String strExceptionMessage){
		this.strExceptionMessage = strExceptionMessage;
	}
}

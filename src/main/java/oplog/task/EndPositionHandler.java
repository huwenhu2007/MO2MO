package oplog.task;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import oplog.entity.OplogPosition;
import org.apache.log4j.Logger;
import utils.PositionUtils;

/**
 * 位置信息处理器
 *  1. 多生产者，单消费者模式
 */
public class EndPositionHandler implements EventHandler<OplogPosition> {

	private Logger logger = Logger.getLogger(EndPositionHandler.class);
	/**
	 * 任务标记
	 */
	private String strWorkSign;
	/**
	 * 本地位置信息文件存储路径
	 */
	private String strEndLogPosPath;
	/**
	 * 是否开启debug
	 */
	private boolean isDebug;
	/**
	 * 上一次成功持久化的位置信息
	 */
	private OplogPosition successOplogPosition;

	/**
	 * 消费者对象
	 */
	private BatchEventProcessor<OplogPosition> oplogPositionBEP;
	/**
	 * 异常信息
	 */
	private String strExceptionMessage;

	public EndPositionHandler(String strWorkSign, String strEndLogPosPath, boolean isDebug) throws Exception {
		this.strWorkSign = strWorkSign;
		this.strEndLogPosPath = strEndLogPosPath;
		this.isDebug = isDebug;
	}

	/**
	 * 启动位置信息处理器
	 * @throws Exception
	 */
	public void startPosition() throws Exception{
		logger.info(String.format("%s 位置信息处理任务 starting", strWorkSign));
		// 启动消费者
		new Thread(oplogPositionBEP).start();
		logger.info(String.format("%s 位置信息处理任务 started", strWorkSign));
	}

	/**
	 * 位置信息处理
	 * @param oplogPosition
	 * @param l
	 * @param b
	 * @throws Exception
	 */
	public void onEvent(OplogPosition oplogPosition, long l, boolean b) throws Exception {
		if(isDebug){
			logger.info(String.format("%s 当前位置事件%s 队列索引 %d 是否消费完毕%b",strWorkSign, oplogPosition.toString(), l, b));
		}

		setExceptionMessage("");
		String strEndPositionName="";
		try {
			PositionUtils.buildEndPositionFileDir(strWorkSign, strEndLogPosPath, oplogPosition.getStrIP(), oplogPosition.getnPort(), oplogPosition.getStrWorkId());
			strEndPositionName=PositionUtils.buildEndPositionFileName(strEndLogPosPath, oplogPosition.getStrIP(), oplogPosition.getnPort(), oplogPosition.getStrWorkId(), oplogPosition.getStrSign());
			PositionUtils.writeEndPosition(strEndPositionName, oplogPosition.getnTime(), oplogPosition.getnIncrement());
			// 设置位置信息
			successOplogPosition = oplogPosition;

			if(isDebug){
				logger.info(String.format("%s 转发位置信息成功：%s", strWorkSign, oplogPosition.toString()));
			}
		} catch (Exception e) {
			logger.error(String.format("%s %s位置信息持久化失败:%s",strWorkSign,strEndPositionName,e.getMessage()), e);
			// 位置信息处理异常，停止消费任务
			stopPosition();
			setExceptionMessage(e.getMessage());
			throw new RuntimeException(String.format("%s %s位置信息持久化失败:%s",strWorkSign,strEndPositionName,e.getMessage()));
		}
	}

	/**
	 * 停止位置信息处理器
	 */
	public void stopPosition() {
		logger.info(String.format("%s 位置信息处理任务 stop", strWorkSign));
		// 停止消费者线程
		this.oplogPositionBEP.halt();

		logger.info(String.format("%s 位置信息处理任务 stoped", strWorkSign));
	}

	public BatchEventProcessor<OplogPosition> getOplogPositionBEP() {
		return oplogPositionBEP;
	}

	public void setOplogPositionBEP(BatchEventProcessor<OplogPosition> oplogPositionBEP) {
		this.oplogPositionBEP = oplogPositionBEP;
	}

	public OplogPosition getSuccessOplogPosition() {
		return successOplogPosition;
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

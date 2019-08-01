package oplog.task;

import oplog.OplogWorker;
import org.apache.log4j.Logger;
import utils.Utilitys;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 任务运行状态维护
 */
public class WorkerDaemonThread extends Thread {
	
	private Logger logger = Logger.getLogger(WorkerDaemonThread.class);

	/**
	 * 工作任务集合
	 */
	private List<OplogWorker> oplogWorkerList;
	/**
	 * 异常信息
	 */
	private String strExceptionMessage;

	public WorkerDaemonThread(List<OplogWorker> oplogWorkerList) {
		this.oplogWorkerList = oplogWorkerList;
	}

	// 检测开始的起始时间
	private long lCheckStartTime = System.currentTimeMillis();

	@Override
	public void run() {
		while(true){
			try {
				long lNow = System.currentTimeMillis();
				// 每分钟检查一次工作任务
				if (lNow - lCheckStartTime < 3 * 60 * 1000) {
					TimeUnit.MICROSECONDS.sleep(500);
					continue;
				}
				lCheckStartTime = lNow;

				if(oplogWorkerList == null){
					logger.error("任务集合 oplogWorkerList is null");
					return ;
				}

				// 维护任务状态
				int nListSize = oplogWorkerList.size();
				for(int i = 0;i < nListSize;i++){
					OplogWorker oplogWorker = oplogWorkerList.get(i);
					if(!oplogWorker.isStarted()){
						// 任务未启动
						logger.info(oplogWorker.getMongoConfig().toString() + " 任务未启动");
						continue ;
					}

					// 维护事件抓取任务
					OplogFetcherTask oplogFetcherTask = oplogWorker.getOplogFetcherTask();
					if(!oplogFetcherTask.isRuning()){
						// 启动抓取任务
						oplogFetcherTask.startFetch();
					}

					// 维护事件处理器
					ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
					if(eventHandlerList != null){
						int nEventHandlerList = eventHandlerList.size();
						for(int j = 0;j < nEventHandlerList;j++){
							OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
							if(!oplogEventHandler.getOplogEventBEP().isRunning()){
								oplogEventHandler.startDispatch();
							}
						}
					}

					// 维护位置信息处理器
					EndPositionHandler endPositionHandler = oplogWorker.getEndPositionHandler();
					if(!endPositionHandler.getOplogPositionBEP().isRunning()){
						endPositionHandler.startPosition();
					}
				}
				// 重置异常信息
				setExceptionMessage("");
			} catch(Exception e){
				logger.error(String.format("任务守护线程异常:%s", e.getMessage()), e);
				setExceptionMessage(e.getMessage());
			}
		}
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

	public static void main(String[] args) {
		long timeCount= Long.valueOf("1485801722396");
		String strFormat="yyyy-MM-dd HH:mm:ss";
		Date date=new Date(timeCount);
		System.out.println(Utilitys.getFormatDateTime(date, strFormat));
	}
}

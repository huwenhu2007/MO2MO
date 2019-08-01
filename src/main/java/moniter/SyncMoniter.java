package moniter;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.DBObject;
import cuckoo.CuckooThread;
import oplog.OplogWorker;
import oplog.entity.OplogPosition;
import oplog.queue.EndPositionRingBuffer;
import oplog.queue.OplogEventRingBuffer;
import oplog.task.OplogEventHandler;
import oplog.task.WorkerDaemonThread;
import org.bson.types.BSONTimestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 同步监控对象
 * @Author huwenhu
 * @Date 2019/7/25 11:39
 **/
public class SyncMoniter {

    private SyncMoniter(){}

    private static SyncMoniter syncMoniter = new SyncMoniter();

    public static SyncMoniter getInstance(){
        return syncMoniter;
    }

    /**
     * 工作任务集合
     */
    private List<OplogWorker> oplogWorkerList;
    /**
     * 守护线程
     */
    private WorkerDaemonThread workerDaemonThread;
    /**
     * 预警线程
     */
    private CuckooThread cuckooThread;

    /**
     * jetty服务对象
     */
    private JettyServer jettyServer = new JettyServer();


    public void start(int nJettyPort, List<OplogWorker> oplogWorkerList, WorkerDaemonThread workerDaemonThread,
                      CuckooThread cuckooThread) throws Exception{
        this.oplogWorkerList = oplogWorkerList;
        this.workerDaemonThread = workerDaemonThread;
        this.cuckooThread = cuckooThread;
        jettyServer.start(nJettyPort);
    }

    /**
     * 获取任务监控信息
     * @return
     */
    public JSONObject getWorkerMoniter(){

        if(oplogWorkerList == null || oplogWorkerList.size() == 0){
            return new JSONObject();
        }

        JSONObject workerJsonObject = new JSONObject();
        int nListSize = oplogWorkerList.size();
        for(int i = 0;i < nListSize;i++){
            OplogWorker oplogWorker = oplogWorkerList.get(i);
            // 任务标记
            String strSign = oplogWorker.getMongoConfig().toString();
            // 任务信息
            JSONObject jsonObject = new JSONObject();
            // 获取任务状态信息
            JSONObject stateJsonObject = getWorkerState(oplogWorker);
            jsonObject.put("state", stateJsonObject);
            // 获取任务进度信息
            JSONObject rateJsonObject = getWorkerRate(oplogWorker);
            jsonObject.put("rate", rateJsonObject);
            // 获取异常信息
            JSONObject exceptionJsonObject = getExceptionMessage(oplogWorker);
            jsonObject.put("exception", exceptionJsonObject);
            // 获取队列信息
            JSONObject bufferJsonObject = getRingBufferData(oplogWorker);
            jsonObject.put("buffer", bufferJsonObject);
            // 添加任务监控信息
            workerJsonObject.put(strSign, jsonObject);
        }
        return workerJsonObject;
    }

    /**
     * 获取任务状态
     * @param oplogWorker
     * @return
     */
    public JSONObject getWorkerState(OplogWorker oplogWorker){
        // 状态
        JSONObject stateJsonObject = new JSONObject();
        // 任务状态
        boolean bWorkerRunState = oplogWorker.isStarted();
        // 抓取状态
        boolean bFetchRunState = oplogWorker.getOplogFetcherTask().isRuning();
        // 目标转发状态
        JSONObject targetStateJsonObject = new JSONObject();
        ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
        int nEventHandleListSize = eventHandlerList.size();
        for(int j = 0;j < nEventHandleListSize;j++){
            OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
            boolean bBatchEventRunState = oplogEventHandler.getOplogEventBEP().isRunning();
            targetStateJsonObject.put(oplogEventHandler.getStrSign(), bBatchEventRunState);
        }
        // 位置消费状态
        boolean bBatchEventRunState = oplogWorker.getEndPositionHandler().getOplogPositionBEP().isRunning();
        // 添加状态信息
        stateJsonObject.put("w", bWorkerRunState);
        stateJsonObject.put("f", bFetchRunState);
        stateJsonObject.put("t", targetStateJsonObject);
        stateJsonObject.put("p", bBatchEventRunState);
        return stateJsonObject;
    }

    /**
     * 获取任务进度
     * @param oplogWorker
     * @return
     */
    public JSONObject getWorkerRate(OplogWorker oplogWorker){
        // 状态
        JSONObject rateJsonObject = new JSONObject();
        // 抓取位置信息
        OplogPosition fetchOplogPosition = oplogWorker.getOplogFetcherTask().getOplogPosition();
        String strFetchOplogPosition = fetchOplogPosition == null ? "" : fetchOplogPosition.toDateString();
        // 目标转发进度信息
        JSONObject targetRateJsonObject = new JSONObject();
        ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
        int nEventHandleListSize = eventHandlerList.size();
        for(int j = 0;j < nEventHandleListSize;j++){
            OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
            DBObject successDBObject = oplogEventHandler.getOplogDispatcher().getSuccessDBObject();
            BSONTimestamp bTimeStamp = null;
            if(successDBObject != null) {
                bTimeStamp = (BSONTimestamp) successDBObject.get("ts");
            }
            targetRateJsonObject.put(oplogEventHandler.getStrSign(), bTimeStamp == null ? "" : bTimeStamp.toString());
        }
        // 位置消费状态
        OplogPosition successOplogPosition = oplogWorker.getEndPositionHandler().getSuccessOplogPosition();
        String strSuccessOplogPosition = successOplogPosition == null ? "" : successOplogPosition.toDateString();
        // 添加状态信息
        rateJsonObject.put("f", strFetchOplogPosition);
        rateJsonObject.put("t", targetRateJsonObject);
        rateJsonObject.put("p", strSuccessOplogPosition);
        return rateJsonObject;
    }

    /**
     * 获取任务异常信息
     * @param oplogWorker
     * @return
     */
    public JSONObject getExceptionMessage(OplogWorker oplogWorker){
        // 异常
        JSONObject exceptionJsonObject = new JSONObject();
        // 启动异常
        String strWorkerExceptionMessage = oplogWorker.getExceptionMessage();
        // 抓取任务异常信息
        String strFetchExceptionMessage = oplogWorker.getOplogFetcherTask().getExceptionMessage();
        // 目标转发进度异常信息
        JSONObject targetExceptionJsonObject = new JSONObject();
        ArrayList<OplogEventHandler> eventHandlerList = oplogWorker.getOplogEventHandler();
        int nEventHandleListSize = eventHandlerList.size();
        for(int j = 0;j < nEventHandleListSize;j++){
            OplogEventHandler oplogEventHandler = eventHandlerList.get(j);
            String strTargetExceptionMessage = oplogEventHandler.getExceptionMessage();
            targetExceptionJsonObject.put(oplogEventHandler.getStrSign(), strTargetExceptionMessage);
        }
        // 位置消费异常信息
        String strPositionExceptionMessage = oplogWorker.getEndPositionHandler().getExceptionMessage();
        // 守护线程异常信息
        String strDaemonExceptionMessage = workerDaemonThread.getExceptionMessage();
        // 预警线程异常信息
        String strCuckooExceptionMessage = "";
        if(cuckooThread != null){
            strCuckooExceptionMessage = cuckooThread.getExceptionMessage();
        }
        // 添加状态信息
        exceptionJsonObject.put("w", strWorkerExceptionMessage);
        exceptionJsonObject.put("f", strFetchExceptionMessage);
        exceptionJsonObject.put("t", targetExceptionJsonObject);
        exceptionJsonObject.put("p", strPositionExceptionMessage);
        exceptionJsonObject.put("d", strDaemonExceptionMessage);
        exceptionJsonObject.put("c", strCuckooExceptionMessage);
        return exceptionJsonObject;
    }

    /**
     * 获取ringbuffer队列信息
     * @param oplogWorker
     * @return
     */
    public JSONObject getRingBufferData(OplogWorker oplogWorker){
        // ringbuffer队列信息
        JSONObject bufferJsonObject = new JSONObject();
        // 事件队列
        OplogEventRingBuffer oplogEventRingBuffer = oplogWorker.getOplogEventRingBuffer();
        // 位置队列
        EndPositionRingBuffer endPositionRingBuffer = oplogWorker.getEndPositionRingBuffer();

        bufferJsonObject.put("event", oplogEventRingBuffer.toJSONObject());
        bufferJsonObject.put("position", endPositionRingBuffer.toJSONObject());
        return bufferJsonObject;
    }


}

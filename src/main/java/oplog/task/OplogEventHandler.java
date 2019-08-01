package oplog.task;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import oplog.OplogDispatcher;
import oplog.entity.OplogEvent;
import oplog.entity.OplogPosition;
import oplog.lock.PositionQueueLock;
import oplog.queue.EndPositionRingBuffer;
import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;


/**
 * 事件处理器
 *  1. 单生产者，多消费者模式
 *  2. 单生产者，单消费者模式
 * @Author huwenhu
 * @Date 2018/12/20 15:29
 **/
public class OplogEventHandler implements EventHandler<OplogEvent> {

    private Logger logger = Logger.getLogger(OplogEventHandler.class);
    /**
     * 任务标记
     */
    private String strWorkId;
    /**
     * 任务标记封装
     */
    private String strWorkSign;
    /**
     * 当前消费者标识
     */
    private String strSign;

    /**
     * 位置信息队列
     */
    private EndPositionRingBuffer endPositionRingBuffer;
    /**
     * 消费者对象
     */
    private BatchEventProcessor<OplogEvent> oplogEventBEP;
    /**
     * 事件转发对象
     */
    private OplogDispatcher oplogDispatcher;
    /**
     * 多消费者位置信息更新锁
     */
    private PositionQueueLock positionQueueLock;
    /**
     * 目标对象数量
     */
    private int nTargetNum;
    /**
     * 异常信息
     */
    private String strExceptionMessage;
    /**
     * 是否为debug模式
     */
    private boolean isDebug;

    public OplogEventHandler(String strWorkId, String strWorkSign, String strSign, EndPositionRingBuffer endPositionRingBuffer,
                             PositionQueueLock positionQueueLock, int nTargetNum, boolean isDebug) throws Exception {
        this.strWorkId = strWorkId;
        this.strWorkSign = strWorkSign;
        this.strSign = strSign;
        this.endPositionRingBuffer = endPositionRingBuffer;
        this.oplogDispatcher = new OplogDispatcher(strWorkSign);
        this.positionQueueLock = positionQueueLock;
        this.nTargetNum = nTargetNum;
        this.isDebug = isDebug;
    }

    /**
     * 开启转发任务
     */
    public void startDispatch() throws Exception{
        logger.info(String.format("%s-%s 事件转发任务 starting", strWorkSign, strSign));
        // 启动目标对象
        oplogDispatcher.getDMLListener().start();
        // 启动消费者
        new Thread(oplogEventBEP).start();
        logger.info(String.format("%s-%s 事件转发任务 started", strWorkSign, strSign));
    }

    public void onEvent(OplogEvent oplogEvent, long l, boolean b) throws Exception {

        if(isDebug){
            logger.info(String.format("%s-%s 当前事件%s 队列索引 %d 是否消费完毕%b",strWorkSign, strSign,oplogEvent.toString(), l, b));
        }

        if (oplogEvent == null) {
            logger.error(String.format("%s-%s oplogEvent is null", strWorkSign, strSign));
            return ;
        }
        setExceptionMessage("");
        // 初始化目标处理任务数据结果
        boolean storeEndPosition = false;

        try {
            // 目标对象处理任务数据
            oplogDispatcher.dispatch(oplogEvent.getDbObject());
            storeEndPosition = true;

            if(isDebug){
                logger.info(String.format("%s-%s 转发事件成功：%s",strWorkSign, strSign,oplogEvent.toString()));
            }
        } catch (Exception e) {
            logger.error(String.format("%s-%s 事件转发异常：%s", strWorkSign, strSign, e.getMessage()), e);
            // 转发异常则停止消费者线程
            stopDispatch();
            setExceptionMessage(e.getMessage());
            throw new RuntimeException(String.format("%s-%s 事件转发异常：%s", strWorkSign, strSign, e.getMessage()), e);
        }

        if (storeEndPosition) {
            // 获取日志事件位置信息
            BSONTimestamp bTimeStamp = (BSONTimestamp) oplogEvent.getDbObject().get("ts");
//            // 当消费者数量大于1时启用位置信息锁
//            if(nTargetNum > 1) {
//                // 比较上次完成时间与当前事件
//                if (!positionQueueLock.comparePosition(bTimeStamp)) {
//                    if(isDebug){
//                        logger.info(String.format("%s-%s 位置信息过滤：%s",strWorkSign, strSign,oplogEvent.toString()));
//                    }
//                    return;
//                }
//            }

            // 目标保存成功，添加位置信息到位置队列
            OplogPosition endPosition = new OplogPosition(oplogEvent.getStrIP(), oplogEvent.getnPort(), strWorkId, strSign, bTimeStamp.getTime(), bTimeStamp.getInc());
            endPositionRingBuffer.publishData(endPosition);

            if(isDebug){
                logger.info(String.format("%s-%s 添加位置信息到队列：%s",strWorkSign, strSign,endPosition.toString()));
            }

//            logger.info(String.format("%s-%s 位置队列%n %s", strWorkSign, strSign, endPositionRingBuffer.toString()));
        }
    }

    /**
     * 停止转发任务
     */
    public void stopDispatch() {
        logger.info(String.format("%s-%s 事件转发任务 stoping", strWorkSign, strSign));
        // 停止消费者线程
        this.oplogEventBEP.halt();
        // 停止转发引擎
        this.oplogDispatcher.getDMLListener().destroy();

        logger.info(String.format("%s-%s 事件转发任务 stoped", strWorkSign, strSign));
    }

    /**
     * 获取消费者对象
     * @return
     */
    public BatchEventProcessor<OplogEvent> getOplogEventBEP(){
        return oplogEventBEP;
    }

    /**
     * 添加消费者对象
     * @param oplogEventBEP
     */
    public void setOplogEventBEP(BatchEventProcessor<OplogEvent> oplogEventBEP){
        this.oplogEventBEP = oplogEventBEP;
    }

    public OplogDispatcher getOplogDispatcher() {
        return oplogDispatcher;
    }

    public String getStrSign() {
        return strSign;
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

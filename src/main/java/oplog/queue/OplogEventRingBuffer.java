package oplog.queue;


import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import oplog.entity.OplogEvent;
import oplog.entity.OplogEventFactory;
import oplog.task.OplogEventHandler;
import org.apache.log4j.Logger;

/**
 * oplog事件队列
 */
public class OplogEventRingBuffer {

    private Logger logger = Logger.getLogger(OplogEventRingBuffer.class);

    private static final int BUFFER_SIZE = 1024;

    private RingBuffer<OplogEvent> ringBuffer;

    public OplogEventRingBuffer(){
        // 初始化队列
        ringBuffer = RingBuffer.createSingleProducer(new OplogEventFactory(), BUFFER_SIZE, new BlockingWaitStrategy());
    }

    /**
     * 发布任务
     * @param oplogEvent
     */
    public void publishData(OplogEvent oplogEvent) throws Exception {
        long seq = ringBuffer.next();
        OplogEvent oplogEvent1 = ringBuffer.get(seq);
        oplogEvent1.from(oplogEvent);
        ringBuffer.publish(seq);
    }

    /**
     * 创建消费者对象
     * @param eventHandler
     * @return
     */
    public BatchEventProcessor<OplogEvent> createConsumer(OplogEventHandler eventHandler)  throws Exception{
        //创建SequenceBarrier
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();

        //创建消息处理器
        BatchEventProcessor<OplogEvent> transProcessor = new BatchEventProcessor<OplogEvent>(
                ringBuffer, sequenceBarrier, eventHandler);

        //这一步的目的就是把消费者的位置信息引用注入到生产者    如果只有一个消费者的情况可以省略
        ringBuffer.addGatingSequences(transProcessor.getSequence());
        return transProcessor;
    }

    public void clear(){

    }

    @Override
    public String toString() {
        return ringBuffer.toString();
    }

    public JSONObject toJSONObject(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("bufferSize", ringBuffer.getBufferSize());
        jsonObject.put("cursor", ringBuffer.getCursor());
        jsonObject.put("miniGatingSeq", ringBuffer.getMinimumGatingSequence());
        return jsonObject;
    }
}

package oplog.queue;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import oplog.entity.OplogPosition;
import oplog.entity.OplogPositionFactory;
import oplog.task.EndPositionHandler;
import org.apache.log4j.Logger;


public class EndPositionRingBuffer {

	private Logger logger = Logger.getLogger(EndPositionRingBuffer.class);

	private static final int BUFFER_SIZE = 1024;

	private RingBuffer<OplogPosition> ringBuffer;

	public EndPositionRingBuffer(){
		// 初始化队列
		ringBuffer = RingBuffer.createMultiProducer(new OplogPositionFactory(), BUFFER_SIZE, new BlockingWaitStrategy());
	}

	/**
	 * 发布任务
	 * @param oplogPosition
	 */
	public void publishData(OplogPosition oplogPosition) throws Exception {
		long seq = ringBuffer.next();
		OplogPosition task = ringBuffer.get(seq);
		task.from(oplogPosition);
		ringBuffer.publish(seq);
	}

	/**
	 * 创建消费者对象
	 * @param eventHandler
	 * @return
	 */
	public BatchEventProcessor<OplogPosition> createConsumer(EndPositionHandler eventHandler) throws Exception{
		//创建SequenceBarrier
		SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
		//创建消息处理器
		BatchEventProcessor<OplogPosition> transProcessor = new BatchEventProcessor<OplogPosition>(
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

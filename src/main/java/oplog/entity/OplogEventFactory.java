package oplog.entity;

import com.lmax.disruptor.EventFactory;

/**
 * @Author huwenhu
 *          事件工厂
 * @Date 2018/12/4 15:08
 **/
public class OplogEventFactory implements EventFactory<OplogEvent> {
    public OplogEvent newInstance(){
        return new OplogEvent();
    }
}

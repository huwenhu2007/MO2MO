package oplog.entity;

import com.lmax.disruptor.EventFactory;

/**
 * @Author huwenhu
 *          位置对账工厂
 * @Date 2018/12/4 15:08
 **/
public class OplogPositionFactory implements EventFactory<OplogPosition> {
    public OplogPosition newInstance(){
        return new OplogPosition();
    }
}

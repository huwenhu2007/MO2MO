package oplog.lock;

import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 位置信息添加到队列使用的锁
 * @Author huwenhu
 * @Date 2019/7/23 9:15
 **/
public class PositionQueueLock {

    private Logger logger = Logger.getLogger(PositionQueueLock.class);

    private ReentrantLock lock = new ReentrantLock();

    /**
     * 上次成功的位置时间
     */
    private BSONTimestamp successTimeStamp;

    /**
     * 比较当前位置与上一次位置
     * @param bTimeStamp
     * @return
     */
    public boolean comparePosition(BSONTimestamp bTimeStamp){
        try {
            lock.lock();
            if(successTimeStamp == null){
                successTimeStamp = bTimeStamp;
                return true;
            }
            // 获取上一次成功的位置信息
            // 当前位置的时间小于或者等于上一次位置的时间则无需保存
            if (successTimeStamp.getTime() > bTimeStamp.getTime()) {
                return false;
            }
            // 当前位置与上一次位置时间相同，当前位置自增小于或者等于上一次位置则无需保存
            else if (successTimeStamp.getTime() == bTimeStamp.getTime() &&
                    successTimeStamp.getInc() >= bTimeStamp.getInc()) {
                return false;
            }

            successTimeStamp = bTimeStamp;
            return true;
        }finally {
            lock.unlock();
        }
    }



}

package cuckoo;

import com.alibaba.fastjson.JSONObject;
import moniter.SyncMoniter;
import oplog.OplogWorker;
import oplog.task.EndPositionHandler;
import oplog.task.OplogEventHandler;
import oplog.task.OplogFetcherTask;
import oplog.task.WorkerDaemonThread;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 预警线程
 * @Author huwenhu
 * @Date 2019/7/26 16:31
 **/
public class CuckooThread extends Thread {

    private Logger logger = Logger.getLogger(CuckooThread.class);

    /**
     * 异常信息
     */
    private String strExceptionMessage;
    /**
     * 预警接口
     */
    private CuckooInterface cuckooInterface;
    /**
     * 配置信息
     */
    private JSONObject cuckooJSONObject;

    public CuckooThread(CuckooInterface cuckooInterface, JSONObject cuckooJSONObject) {
        this.cuckooInterface = cuckooInterface;
        this.cuckooJSONObject = cuckooJSONObject;
    }

    // 检测开始的起始时间
    private long lCheckStartTime = System.currentTimeMillis();

    @Override
    public void run() {
        while(true){
            try {
                long lNow = System.currentTimeMillis();
                // 每分钟检查一次是否需要预警
                if (lNow - lCheckStartTime < 60 * 1000) {
                    TimeUnit.MICROSECONDS.sleep(500);
                    continue;
                }
                lCheckStartTime = lNow;

                JSONObject cuckoo = new JSONObject();
                // 获取监控信息
                JSONObject jsonObject = SyncMoniter.getInstance().getWorkerMoniter();
                Set<String> set = jsonObject.keySet();
                Iterator<String> iterator = set.iterator();
                while(iterator.hasNext()){
                    JSONObject cuckooJSON = new JSONObject();

                    String key = iterator.next();
                    JSONObject data = jsonObject.getJSONObject(key);
                    JSONObject exceptionJSONObject = data.getJSONObject("exception");
                    String w = exceptionJSONObject.getString("w");
                    if(StringUtils.isNotBlank(w)) {
                        cuckooJSON.put("启动异常", w);
                    }
                    String f = exceptionJSONObject.getString("f");
                    if(StringUtils.isNotBlank(f)) {
                        cuckooJSON.put("抓取异常", f);
                    }
                    JSONObject tCuckooJSON = new JSONObject();
                    JSONObject tJSONObject = exceptionJSONObject.getJSONObject("t");
                    // 获取目标状态
                    Set<String> tSet = tJSONObject.keySet();
                    Iterator<String> tIterator = tSet.iterator();
                    while(tIterator.hasNext()){
                        String tKey = tIterator.next();
                        String tValue = tJSONObject.getString(tKey);
                        if(StringUtils.isNotBlank(tValue)) {
                            tCuckooJSON.put(tKey, tValue);
                        }
                    }
                    if(!tCuckooJSON.isEmpty()) {
                        cuckooJSON.put("目标异常", tCuckooJSON);
                    }
                    String p = exceptionJSONObject.getString("p");
                    if(StringUtils.isNotBlank(p)) {
                        cuckooJSON.put("位置消费异常", p);
                    }
                    String d = exceptionJSONObject.getString("d");
                    if(StringUtils.isNotBlank(d)) {
                        cuckooJSON.put("守护线程异常", d);
                    }
                    String c = exceptionJSONObject.getString("c");
                    if(StringUtils.isNotBlank(c)) {
                        cuckooJSON.put("预警线程异常", c);
                    }

                    if(!cuckooJSON.isEmpty()){
                        cuckoo.put(key, cuckooJSON);
                    }
                }
                if(!cuckoo.isEmpty()) {
                    // 异常缓存已经存在数据则无需再次通知
                    if(!ExceptionCache.getInstance().isExist(cuckoo.toString())) {
                        cuckooJSONObject.put("strContent", cuckoo.toString());
                        cuckooInterface.exceptionNotice(cuckooJSONObject);
                        ExceptionCache.getInstance().put(cuckoo.toString(), true);
                    }
                }
                setExceptionMessage("");
            } catch(Exception e){
                logger.error(String.format("预警线程异常:%s", e.getMessage()), e);
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



}

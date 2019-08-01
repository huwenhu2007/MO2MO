package oplog.listener;

import com.alibaba.fastjson.JSONObject;
import oplog.entity.DMLEvent;
import org.apache.commons.lang.StringUtils;

/**
 * @Author huwenhu
 * @Date 2019/8/1 11:01
 **/
public abstract class DMLListenerAbs implements DMLListener{

    @Override
    public abstract void init(String strWorkSign, String strSign, JSONObject jsonObject, boolean isDebug) throws Exception;

    @Override
    public abstract void start() throws Exception ;

    @Override
    public abstract void onEvent(DMLEvent data) throws Exception ;

    @Override
    public abstract void destroy() ;

    /**
     * 转换库表名称
     * @param event
     * @return
     */
    public JSONObject ruleChangeName(DMLEvent event, JSONObject jsonDBObject, JSONObject jsonTableObject){
        // 创建返回对象
        JSONObject jsonObject = new JSONObject();
        // 获取事件中的库表名称
        String strDBName=event.getStrDBName();
        String strCollectionName=event.getStrCollectionName();
        if(StringUtils.isEmpty(strDBName)||StringUtils.isEmpty(strCollectionName)){
            return jsonObject;
        }
        // 库名转换
        if(jsonDBObject != null && jsonDBObject.containsKey(strDBName)){
            strDBName = jsonDBObject.getString(strDBName);
        }
        jsonObject.put("strDBName", strDBName);

        // 表名转换
        if(jsonTableObject != null && jsonTableObject.containsKey(strCollectionName)){
            strCollectionName = jsonDBObject.getString(strCollectionName);
        }
        jsonObject.put("strCollectionName", strCollectionName);

        return jsonObject;
    }
}

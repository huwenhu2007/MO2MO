package cuckoo;

import com.alibaba.fastjson.JSONObject;

/**
 * 预警接口
 * @Author huwenhu
 * @Date 2019/7/26 15:22
 **/
public interface CuckooInterface {
    /**
     * 初始化预警对象
     */
    public void init(JSONObject jsonObject) throws Exception;

    /**
     * 异常通知
     */
    public void exceptionNotice(JSONObject jsonObject) throws Exception;

}

package oplog.listener;


import com.alibaba.fastjson.JSONObject;
import oplog.MongoConfig;
import oplog.entity.DMLEvent;

/**
 * 	
	 INSERT
	 UPDATE
	 DELETE
	
 * @author Administrator
 *
 */
public interface DMLListener {
	/**
	 * 初始化目标对象数据
	 * @param strWorkSign		任务标记
	 * @param strSign		消费者编号
	 * @param jsonObject	配置信息
	 * @param isDebug       debug模式
	 * @throws Exception
	 */
	public void init(String strWorkSign, String strSign, JSONObject jsonObject, boolean isDebug) throws Exception;

	/**
	 * 启动目标对象
	 * @throws Exception
	 */
	public void start() throws Exception;

	/**
	 * 转发数据到目标
	 * @param data
	 * @throws Exception
	 */
	public void onEvent(DMLEvent data) throws Exception;

	/**
	 * 停止目标服务
	 */
	public void destroy();
	
}

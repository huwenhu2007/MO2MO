
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import cuckoo.CuckooFactory;
import cuckoo.CuckooInterface;
import cuckoo.CuckooThread;
import log.LogConfiguration;
import moniter.SyncMoniter;
import oplog.MongoConfig;
import oplog.OplogWorker;
import oplog.task.WorkerDaemonThread;
import org.apache.commons.lang.StringUtils;
import utils.OperatingSystemUtils;
import utils.Utilitys;
import org.apache.log4j.Logger;
import zk.ZKClient;
import java.nio.charset.Charset;
import java.util.*;

/**
 * 同步启动类
 */
public class MO2MOJAPP {
	
	private static Logger logger = Logger.getLogger(MO2MOJAPP.class);

	/**
	 * 设置为单例
	 */
	private MO2MOJAPP(){}
	
	private static class MO2MOJAPPSingle{
		public static MO2MOJAPP mo2mojapp = new MO2MOJAPP();
	}
	
	public static MO2MOJAPP getInstance(){
		return MO2MOJAPPSingle.mo2mojapp;
	}

	/**
	 * 同步任务集合
	 */
	private List<OplogWorker> oplogWorkerList=new ArrayList<OplogWorker>();

	/**
	 * 系统信息
	 */
	private static OperatingSystemUtils operatingSystemUtils = new OperatingSystemUtils();

	/**
	 * 预警线程
	 */
	private CuckooThread cuckooThread;

	/**
	 * 守护线程
	 */
	private WorkerDaemonThread workerDaemonThread;

	/**
	 * 启动任务
	 * @throws Exception
	 */
	public void start() throws Exception {
		// main函数入口java文件编码
		logger.info("file.encoding=" + System.getProperty("file.encoding"));
		// 转换为字节数组默认编码
		logger.info("Default Charset=" + Charset.defaultCharset());
		// 转换为字节数组使用编码
 		logger.info("Default Charset in Use=" + operatingSystemUtils.getDefaultCharSet());
 		// 判断进程是否已经启动
 		if(operatingSystemUtils.checkJavaAppIsRuning(operatingSystemUtils.getSelfPID(),operatingSystemUtils.getProcessName())){
 			logger.error(operatingSystemUtils.getProcessName() + " already runing");
			return;
		}
		// 获取配置信息
		String strConfigJSON = Utilitys.getJSONFileData("/config.json");
		if(StringUtils.isBlank(strConfigJSON)){
			throw new RuntimeException(String.format("% 配置信息不存在", "config.properties"));
		}
 		JSONObject jsonConfig = JSONObject.parseObject(strConfigJSON);
 		List<MongoConfig> list = getMongoConfig(jsonConfig);
 		// 生成工作对象
 		int nListSize = list.size();
 		for (int i = 0; i < nListSize; i++) {
			MongoConfig mongoConfig = list.get(i);
			OplogWorker worker = new OplogWorker(mongoConfig);
 			if(worker != null){
 				oplogWorkerList.add(worker);
 			}
		}
		// 启动工作对象
		for (int i = 0; i < oplogWorkerList.size(); i++) {
			OplogWorker worker=oplogWorkerList.get(i);
			worker.start();
		}
		// 启动任务守护线程
		workerDaemonThread=new WorkerDaemonThread(this.oplogWorkerList);
		workerDaemonThread.start();

		// 预警对象
		int nCuckoo = jsonConfig.getIntValue("nCuckoo");
		if(nCuckoo == 1){
			logger.info(operatingSystemUtils.getProcessName() + " 预警线程启动开始");
			String strCuckooType = jsonConfig.getString("strCuckooJSON");
			CuckooInterface cuckooInterface = CuckooFactory.getInstance().getCuckooService(strCuckooType);
			JSONObject cuckooJSONObject = jsonConfig.getJSONObject("cuckooJSONObject");
			cuckooInterface.init(cuckooJSONObject);
			cuckooThread=new CuckooThread(cuckooInterface, cuckooJSONObject);
			cuckooThread.start();
			logger.info(operatingSystemUtils.getProcessName() + " 预警线程启动完成");
		}

		// 实例化任务监控信息对象
		int nMoniterState = jsonConfig.getIntValue("nMoniterState");
		if(nMoniterState == 1){
			int nJettyPort = jsonConfig.getIntValue("nJettyPort");
			logger.info(String.format("%s-%d 监控启动开始",operatingSystemUtils.getProcessName(),nJettyPort));
			SyncMoniter.getInstance().start(nJettyPort, this.oplogWorkerList, workerDaemonThread, cuckooThread);
		}
	}

	/**
	 * 获取同步配置信息
	 * @param jsonConfig     本地配置信息
	 */
	private List<MongoConfig> getMongoConfig(JSONObject jsonConfig) throws Exception {
		// 默认读取本地配置
		String strConfigMode = jsonConfig.getString("strConfigMode");
		if("zk".equals(strConfigMode)){
			// 使用zk配置信息
			// zk集群信息
			String strZKClientDomain = jsonConfig.getString("strZKClientDomain");
			// zk根节点
			String strRootNodeName = jsonConfig.getString("strRootNodeName");
			// 连接zk集群
			ZKClient.getInstance().startZK(strZKClientDomain);
			// 判断进程节点是否存在
			String strProcessNode = new StringBuilder("/").append(strRootNodeName).append("/").append(operatingSystemUtils.getProcessName()).toString();
			// 判断进程节点是否存在
			if(!ZKClient.getInstance().isExist(strProcessNode)){
				throw new RuntimeException(String.format("%s进程节点不存在", strProcessNode));
			}
			// 添加当前节点及子节点事件监听
			ZKClient.getInstance().onListenerChildren(strProcessNode);
			// 节点添加重连监听
			ZKClient.getInstance().addSessionListener(strProcessNode);
			// 获取进程节点信息
			JSONObject processJSONObject = ZKClient.getInstance().getNodeJSONData(strProcessNode);
			// 日志位置信息本地目录
			String strEndLogPosPath = processJSONObject.getString("strEndLogPosPath");
			// 是否启用监控
			int nMoniterState = processJSONObject.getIntValue("nMoniterState");
			jsonConfig.put("nMoniterState", nMoniterState);
			if(nMoniterState == 1){
				// jetty端口信息
				int nJettyPort = processJSONObject.getIntValue("nJettyPort");
				jsonConfig.put("nJettyPort", nJettyPort);
			}
			// 添加预警信息
			int nCuckoo = processJSONObject.getIntValue("nCuckoo");
			if(nCuckoo == 1){
				String strCuckooJSON = processJSONObject.getString("strCuckooJSON");
				String cuckooJSONNode = new StringBuilder("/").append(strRootNodeName).append("/").append(strCuckooJSON).toString();
				JSONObject cuckooJSONObject = ZKClient.getInstance().getNodeJSONData(cuckooJSONNode);
				jsonConfig.put("cuckooJSONObject", cuckooJSONObject);
			}
			// 获取进程中的任务配置信息
			List<JSONObject> list = ZKClient.getInstance().getChildNodeJSONDataList(strProcessNode);
			int nListSize = list.size();
			List<MongoConfig> zkList = new ArrayList<>();
			for(int i = 0;i < nListSize;i++){
				JSONObject jsonObject = list.get(i);
				// 生成目标配置JSON对象
				JSONArray jsonArray = new JSONArray();
				String strWorkerId = jsonObject.getString("strWorkerId");
				String strNodePath = new StringBuilder(strProcessNode).append("/").append(strWorkerId).toString();
				List<JSONObject> listChild = ZKClient.getInstance().getChildNodeJSONDataList(strNodePath);
				int nListChildSize = listChild.size();
				for(int j = 0;j < nListChildSize;j++) {
					JSONObject jsonObjectChild = listChild.get(j);
					jsonArray.add(jsonObjectChild);
				}
				jsonObject.put("strDMLTargetJSONArray", jsonArray);
				// 添加位置信息
				jsonObject.put("strEndLogPosPath", strEndLogPosPath);
				// 组装配置信息
				MongoConfig mongoConfig = new MongoConfig(jsonObject);
				zkList.add(mongoConfig);
			}
			return zkList;
		}

		// 使用本地配置
		List<MongoConfig> localList = new ArrayList<>();
		// 获取位置信息路径
		String strEndLogPosPath = jsonConfig.getString("strEndLogPosPath");
		// 获取源数据数量
		JSONArray sourceConfig = jsonConfig.getJSONArray("sourceConfig");
		int nLength = sourceConfig.size();
		for(int i = 0;i < nLength;i++){
			JSONObject jsonObject = sourceConfig.getJSONObject(i);
			if(!jsonObject.containsKey("strWorkerId")){
				logger.info(String.format("strWorkerId不存在"));
				// id不存在则跳过
				continue ;
			}
			// 获取目标配置文件信息
			String strDMLListenerJSON = jsonObject.getString("strDMLListenerJSON");
			// 生成目标配置JSON对象
			String strTargetJSON = Utilitys.getJSONFileData(strDMLListenerJSON);
			if(StringUtils.isBlank(strTargetJSON)){
				throw new RuntimeException(String.format("%s目标配置信息不存在", jsonObject.toString()));
			}
			JSONArray jsonArray = JSONObject.parseArray(strTargetJSON);
			jsonObject.put("strDMLTargetJSONArray", jsonArray);
			// 添加位置信息
			jsonObject.put("strEndLogPosPath", strEndLogPosPath);
			// 组装MongoConfig对象
			MongoConfig mongoConfig = new MongoConfig(jsonObject);
			localList.add(mongoConfig);
		}
		// 加载预警信息
		int nCuckoo = jsonConfig.getIntValue("nCuckoo");
		if(nCuckoo == 1){
			String strCuckooJSON = jsonConfig.getString("strCuckooJSON");
			String path = new StringBuilder("/").append(strCuckooJSON).append(".json").toString();
			String strCuckooJSONData = Utilitys.getJSONFileData(path);
			JSONObject cuckooJSONObject = JSONObject.parseObject(strCuckooJSONData);
			jsonConfig.put("cuckooJSONObject", cuckooJSONObject);
		}

		return localList;
	}

	/**
	 * 停止任务
	 */
	public void stop(){
		for (int i = 0; i < oplogWorkerList.size(); i++) {
			OplogWorker worker=oplogWorkerList.get(i);
			worker.stop();
		}
	}

	public static void main(String[] args) {
		// 设置log4j日志配置
		LogConfiguration.initLog(operatingSystemUtils.getProcessName());
		// 获取任务启动对象
		MO2MOJAPP m = MO2MOJAPP.getInstance();
		try {
			// 启动任务
			m.start();
		} catch (Exception e) {
			m.stop();
			logger.error(e.getMessage(), e);
		}
	}
	
}

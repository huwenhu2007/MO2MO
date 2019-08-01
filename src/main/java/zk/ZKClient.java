package zk;

import com.alibaba.fastjson.JSONObject;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZKClient {
	
	private static final Logger logger = Logger.getLogger(ZKClient.class);
	
	public static ZKClient getInstance() {
		return ZKClientHolder.instance;
	}
	
	private static class ZKClientHolder {
		private static ZKClient instance = new ZKClient();
	}
	
	private ZKClient() {}
	
	CuratorFramework client = null;
	
	/**
	 * 启动zk客户端
	 * @param strClientDomain	zk服务器的ip和端口配置
	 */
	public void startZK(String strClientDomain){
		// 设置zk的重试规则（按照1秒进行渐进重试，最多重试3次）
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		// 使用流式的方式创建客户端
		client = CuratorFrameworkFactory.builder()
                .connectString(strClientDomain)
                .sessionTimeoutMs(3000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
		// 开启客户端
        client.start();
	}
	
	public boolean isStarted(){
		if(client == null){
			return false;
		}
		return client.isStarted();
	}

	public void stop()
	{
		if(client == null)
		{
			return;
		} else
		{
			removeSessionListener();
			client.close();
			return;
		}
	}

	// zk会话监听
	SessionConnectionListener sessionConnectionListener;

	public void addSessionListener(String strPath)
	{
		sessionConnectionListener = new SessionConnectionListener(strPath, "");
		client.getConnectionStateListenable().addListener(sessionConnectionListener);
	}

	public void removeSessionListener()
	{
		if(sessionConnectionListener == null)
		{
			return;
		} else
		{
			client.getConnectionStateListenable().removeListener(sessionConnectionListener);
			return;
		}
	}

	/**
	 * 获取子节点信息
	 * @param strNode      节点路径
	 * @return
	 * @throws Exception
	 */
	public List<String> getChildNode(String strNode) throws Exception {
		List<String> list = client.getChildren().forPath(strNode);
		return list;
	}

	/**
	 * 获取节点数据
	 * @param strNode		节点路径
	 * @return
	 * @throws Exception
	 */
	public String getNodeStrData(String strNode) throws Exception {
		byte[] bData = client.getData().forPath(strNode);
		String strData = new String(bData);
		return strData;
	}


	/**
	 * 获取节点数据并返回json
	 * @param strNode
	 * @return
	 * @throws Exception
	 */
	public JSONObject getNodeJSONData(String strNode) throws Exception {
		String strData = getNodeStrData(strNode);
		JSONObject jsonData = JSONObject.parseObject(strData);
		return jsonData;
	}

	/**
	 * 获取子节点字符串内容
	 * @param strNode		节点名称
	 * @return
	 * @throws Exception
	 */
	public List<String> getChildNodeStrDataList(String strNode) throws Exception {
		List<String> list = getChildNode(strNode);
		int nListSize = list.size();
		// 添加返回字符串数据
		ArrayList<String> childNodeStrDataList = new ArrayList<>();
		for(int i = 0;i < nListSize;i++){
			String strChildNodeName = list.get(i);
			String strNodePath = strNode + "/" + strChildNodeName;
			String strChildNodeStrData = getNodeStrData(strNodePath);
			childNodeStrDataList.add(strChildNodeStrData);
		}
		return childNodeStrDataList;
	}

	/**
	 * 获取子节点JSON数据
	 * @param strNode        节点路径
	 * @return
	 * @throws Exception
	 */
	public List<JSONObject> getChildNodeJSONDataList(String strNode) throws Exception {
		List<String> list = getChildNode(strNode);
		int nListSize = list.size();
		// 添加返回JSON数据
		ArrayList<JSONObject> childNodeJSONDataList = new ArrayList<>();
		for(int i = 0;i < nListSize;i++){
			String strChildNodeName = list.get(i);
			String strNodePath = strNode + "/" + strChildNodeName;
			JSONObject strChildNodeJSONData = getNodeJSONData(strNodePath);
			childNodeJSONDataList.add(strChildNodeJSONData);
		}
		return childNodeJSONDataList;
	}

	/**
	 * 创建临时节点
	 * @param strPath		节点路径
	 * @param strMessage	节点信息
	 * @throws Exception
	 */
	public void createTemporaryNode(String strPath, String strMessage) throws Exception {
		if(isExist(strPath)){
			return ;
		}
		client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.EPHEMERAL)
			.forPath(strPath,strMessage.getBytes());
	}
	
	/**
	 * 创建永久节点
	 * @param strPath		节点路径
	 * @param strMessage	节点信息
	 * @throws Exception
	 */
	public void createPersistentNode(String strPath, String strMessage) throws Exception {
		if(isExist(strPath)){
			return ;
		}
		client.create()
			.creatingParentsIfNeeded()
			.withMode(CreateMode.PERSISTENT)
			.forPath(strPath,strMessage.getBytes());
	}
	
	/**
	 * 判断节点是否存在
	 * @param strPath		节点路径
	 * @return
	 * @throws Exception
	 */
	public boolean isExist(String strPath) throws Exception {
		Stat stat = client.checkExists().forPath(strPath);
		if(stat == null){
			return false;
		}
		return true;
	}
	
	/**
	 * 修改节点信息
	 * @param strPath		节点路径
	 * @param strMessage	信息
	 * @throws Exception
	 */
	public void updateNode(String strPath, String strMessage) throws Exception {
		Stat stat = new Stat();
		client.getData().storingStatIn(stat).forPath(strPath);
		client.setData().withVersion(stat.getVersion()).forPath(strPath, strMessage.getBytes());
	}
	
	/**
	 * 删除节点信息
	 * @param strPath		节点路径
	 * @throws Exception
	 */
	public void deleteNode(String strPath) throws Exception {
		if(!isExist(strPath)){
			return ;
		}
		
		client.delete()
			.guaranteed()				// 删除失败，则客户端会持续删除
			.deletingChildrenIfNeeded()
			.withVersion(-1)			// 直接删除，无需考虑版本
			.forPath(strPath);
	}
	
	public void onListenerChildren(String strPath) throws Exception {
		// 子节点监听
		final PathChildrenCache pccache = new PathChildrenCache(client,strPath,true);
		// 启动监听
		pccache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
		// 添加监听事件
		pccache.getListenable().addListener(new PathChildrenCacheListener() {
			
			@Override
			public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
					throws Exception {
				
//				if(!M2M4JApp.isZKClient){
//					logger.info("[path:"+event.getData().getPath()+"] 节点非zk事件模式");
//					return ;
//				}
				
				// 事件类型
				switch (event.getType()) {
					// 添加
					case CHILD_ADDED:
						logger.info("[path:"+event.getData().getPath()+"] 添加节点");
					// 修改
					case CHILD_UPDATED:
						// 获取节点对象
						ChildData cd = event.getData();
						// 获取节点路径
						String path = cd.getPath();
						// 获取节点名称
						String[] arrPath = path.split("/");
						String nodeName = arrPath[arrPath.length - 1];
						// 获取任务标记
						String[] arrNode = nodeName.split("\\|");
						String ip = arrNode[0];
						String port = arrNode[1];
						String slaveid = arrNode[2];
						// 获取节点数据
						String data = new String(cd.getData());
						logger.info("[path:"+event.getData().getPath()+"] 修改节点" + nodeName + " 任务操作 " + data);
						// 获取操作指令
						JSONObject jsonObject = JSONObject.parseObject(data);
						String code = jsonObject.getString("code");

						break;
					// 删除
					case CHILD_REMOVED:
						logger.info("[path:"+event.getData().getPath()+"] 删除节点");
						break;
					default:
						break;
				}
				
			}
		});
		
	}
	
	private Lock lock = new ReentrantLock();

	
	public static void main(String[] args) {
		ZKClient zkClient = new ZKClient();
		zkClient.startZK("127.0.0.1:2181");
		try {
			
			zkClient.onListenerChildren("/M2M4J_6.0");
			for(int i = 0;i < 10;i++){
				if(zkClient.isExist("/M2M4J_6.0/123")){
					zkClient.updateNode("/M2M4J_6.0/123", "22222");
				} else {
					zkClient.createPersistentNode("/M2M4J_6.0/123", "");
				}
				Thread.sleep(1000);
				System.out.println("-------------");
			}
			Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

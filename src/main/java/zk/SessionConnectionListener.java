package zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.log4j.Logger;


/**
 * @Author huwenhu
 * @Date 2019/1/30 16:46
 **/
public class SessionConnectionListener implements ConnectionStateListener {

    private final Logger logger = Logger.getLogger(SessionConnectionListener.class);
    private String path;
    private String data;

    public SessionConnectionListener(String path, String data)
    {
        this.path = path;
        this.data = data;
    }

    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState)
    {
        if(connectionState == ConnectionState.LOST)
        {
            logger.info("[负载均衡失败]zk session超时");
            do
            {
                try
                {
                    if(curatorFramework.getZookeeperClient().blockUntilConnectedOrTimedOut())
                    {
                        logger.info("[负载均衡修复]重连zk成功");
                        break;
                    }
                }
                catch(InterruptedException e)
                {
                    logger.error((new StringBuilder()).append("[负载均衡失败]").append(e.getMessage()).toString(), e);
                    break;
                }
                catch(Exception e)
                {
                    logger.error((new StringBuilder()).append("[负载均衡失败]").append(e.getMessage()).toString(), e);
                }
                try
                {
                    Thread.currentThread();
                    Thread.sleep(20L);
                }
                catch(InterruptedException e) { }
            } while(true);
        }
    }

}

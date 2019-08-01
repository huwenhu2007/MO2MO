package moniter;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * jetty 服务
 * @Author huwenhu
 * @Date 2019/7/25 17:18
 **/
public class JettyServer {

    /**
     * 启动jetty容器
     * @param nPort
     * @throws Exception
     */
    public void start(int nPort) throws Exception {
        Server server = new Server(nPort);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        // Or ServletContextHandler.NO_SESSIONS
        context.setContextPath("/");
        server.setHandler(context);

        // http://localhost:8080/hello
        context.addServlet(new ServletHolder(new MoniterHandler()), "/moniter");
        server.start();
        server.join();
    }


}

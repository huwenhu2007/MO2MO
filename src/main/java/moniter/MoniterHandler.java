package moniter;

import com.alibaba.fastjson.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @Author huwenhu
 * @Date 2019/7/25 17:20
 **/
public class MoniterHandler extends HttpServlet{


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        JSONObject moniterJSONObject = SyncMoniter.getInstance().getWorkerMoniter();
        PrintWriter out = resp.getWriter();
        out.print(moniterJSONObject.toString());
        out.close();
    }
}

package cuckoo;/**
 * Created by huwenhu on 2019/8/2.
 */

import com.alibaba.fastjson.JSONObject;
import utils.HttpConnectionManager;

import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Properties;

/**
 * @Author huwenhu
 * @Date 2019/8/2 16:02
 **/
public class DingdingCuckooService implements CuckooInterface{

    @Override
    public void init(JSONObject jsonObject) throws Exception{

    }

    @Override
    public void exceptionNotice(JSONObject jsonObject) throws Exception{
        // 获取配置信息
        String strDingdingUrl = jsonObject.getString("strDingdingUrl");
        String strContent = jsonObject.getString("strContent");
        // 发送钉钉预警
        JSONObject strBody = new JSONObject();
        strBody.put("msgtype", "text");
        JSONObject text = new JSONObject();
        text.put("content", strContent);
        strBody.put("text", text);

        JSONObject at = new JSONObject();
        at.put("isAtAll", "false");
        strBody.put("at", at);
        HttpConnectionManager.getInstance().postForText(strDingdingUrl, strBody.toString());
    }

}

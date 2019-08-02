package cuckoo;

import com.alibaba.fastjson.JSONObject;
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
 * email预警通知对象
 * @Author huwenhu
 * @Date 2019/7/26 15:25
 **/
public class EmailCuckooService implements CuckooInterface {

    private Session session;

    @Override
    public void init(JSONObject jsonObject) throws Exception{
        Properties props = new Properties();
        // 开启debug调试
        props.setProperty("mail.debug", jsonObject.getString("bDebug"));
        // 发送服务器需要身份验证
        props.setProperty("mail.smtp.auth", jsonObject.getString("bAuth"));
        // 设置邮件服务器主机名
        props.setProperty("mail.smtp.host", jsonObject.getString("strHost"));
        props.setProperty("mail.smtp.port", jsonObject.getString("strPort"));
        // 发送邮件协议名称
        props.setProperty("mail.transport.protocol", jsonObject.getString("strProtocol"));
        // 设置环境信息
        session = Session.getDefaultInstance(props);
    }

    @Override
    public void exceptionNotice(JSONObject jsonObject) throws Exception{

        // 创建邮件内容对象
        Message msg = new MimeMessage(session);
        msg.setSubject("MO2MO Cuckoo");
        // 发送人信息
        InternetAddress[] iaToList = InternetAddress.parse(jsonObject.getString("strToEmail"));
        msg.setRecipients(Message.RecipientType.TO, iaToList); // 收件人
        msg.setFrom(new InternetAddress("<" + jsonObject.getString("strSendEmail") + ">"));
        // 发送内容
        Multipart mainPart = new MimeMultipart();
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        messageBodyPart.setContent(jsonObject.getString("strContent"),"text/html; charset=utf-8");
        // 设置邮件内容
        mainPart.addBodyPart(messageBodyPart);
        msg.setContent(mainPart);
        // 发送邮件对象
        Transport transport = session.getTransport();
        // 连接邮件服务器
        transport.connect(jsonObject.getString("strUserName"), jsonObject.getString("strPassword"));
        // 发送邮件
        transport.sendMessage(msg, msg.getAllRecipients());
        // 关闭连接
        transport.close();
    }


    public static void main(String[] args){
        try{
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("bDebug", "false");
            jsonObject.put("bAuth", "true");
            jsonObject.put("strHost", "smtp.163.com");
            jsonObject.put("strPort", "25");
            jsonObject.put("strProtocol", "smtp");
            jsonObject.put("strSubject", "MO2MO Cuckoo_1");
            jsonObject.put("strSendEmail", "");
            jsonObject.put("strUserName", "");
            jsonObject.put("strPassword", "");
            jsonObject.put("strToEmail", "");
            jsonObject.put("strContent", "通知地址");

            EmailCuckooService emailCuckooService = new EmailCuckooService();
            emailCuckooService.init(jsonObject);

            emailCuckooService.exceptionNotice(jsonObject);


        } catch(Exception e){
            e.printStackTrace();
        }
    }
}

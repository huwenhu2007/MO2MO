package log;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

public class LogConfiguration {
	
	private static Logger logger = Logger.getLogger(LogConfiguration.class);
	
	public static void initLog(String logFileName){
        //声明日志文件存储路径以及文件名、格式
        Properties prop = new Properties();
        // 设置日志级别
        prop.setProperty("log4j.rootLogger","info,ErrorLogFile,FILE,stdout");
        prop.setProperty("log4j.appender.file.encoding","UTF-8" );
        // 输出控制台
        prop.setProperty("log4j.appender.stdout","org.apache.log4j.ConsoleAppender");
        prop.setProperty("log4j.appender.stdout.layout","org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.stdout.layout.ConversionPattern","%d{yyyy-MM-dd HH:mm:ss:SSS}%5p ["+logFileName+",%C,%L]: %m%n");
        // 设置错误级别日志配置
        prop.setProperty("log4j.appender.ErrorLogFile","org.apache.log4j.DailyRollingFileAppender");
        prop.setProperty("log4j.appender.ErrorLogFile.File","/logs/"+logFileName+"_ERROR.log");
        prop.setProperty("log4j.appender.ErrorLogFile.DatePattern","'.'yyyy-MM-dd");
        prop.setProperty("log4j.appender.ErrorLogFile.Append","true");
        prop.setProperty("log4j.appender.ErrorLogFile.Threshold","error");
        prop.setProperty("log4j.appender.ErrorLogFile.layout","org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.ErrorLogFile.layout.ConversionPattern","%d{yyyy-MM-dd HH:mm:ss:SSS}%5p ["+logFileName+",%C,%L]: %m%n");
        // 设置信息级别的日志配置
        prop.setProperty("log4j.appender.FILE","org.apache.log4j.DailyRollingFileAppender");
        prop.setProperty("log4j.appender.FILE.File","/logs/"+logFileName+".log");
        prop.setProperty("log4j.appender.FILE.DatePattern","'.'yyyy-MM-dd");
        prop.setProperty("log4j.appender.FILE.Append","true");
        prop.setProperty("log4j.appender.FILE.Threshold","info");
        prop.setProperty("log4j.appender.FILE.layout","org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.FILE.layout.ConversionPattern","%d{yyyy-MM-dd HH:mm:ss:SSS}%5p ["+logFileName+",%C,%L]: %m%n");
        //使配置生效  
        PropertyConfigurator.configure(prop); 
        
        logger.info("log4j init success");
	}
	
	public static void main(String[] args) {
		
		initLog("test");
		
	}
	
}

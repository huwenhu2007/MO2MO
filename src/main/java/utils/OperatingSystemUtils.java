package utils;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class OperatingSystemUtils {

	private static Logger logger = Logger.getLogger(OperatingSystemUtils.class);

	/**
	 * 通过进程id和进程名称检查进程是否已启动
	 * @param strPID			进程id
	 * @param strAppName		进程名称
	 * @return
	 * @throws Exception
	 */
	public boolean checkJavaAppIsRuning(String strPID, String strAppName) throws Exception {
	 	String strOSName = System.getProperties().getProperty("os.name").toUpperCase();
	 	String[] strCommand=null;
 		Runtime runtime = Runtime.getRuntime();
		if (strOSName.contains("WINDOWS")) {
			strCommand=new String[]{"jps"};
		} else if (strOSName.contains("LINUX") || strOSName.contains("MAC")) {
			strCommand=new String[]{"sh","-c","ps -e -opid,command |grep java"};
		} else {
			throw new RuntimeException("cannot be identified strOSName=" + strOSName);
		}
		InputStream in = runtime.exec(strCommand).getInputStream();
		BufferedReader b = new BufferedReader(new InputStreamReader(in));
		String line = null;
		while ((line = b.readLine()) != null) {
			line=line.trim();
			String[] lineSlice=line.split(" ");
			if(strPID.equals(lineSlice[0])==false){
				if (line.indexOf(strAppName) >= 0) {
					logger.info("checkJavaAppIsRuning exists =" + line);
					return true;
				}
			}
			 
		}
		return false;
	}

	/**
	 * 缓存当前进程id
	 */
	private String strPId = "";

	/**
	 * 获取当前进程id
	 * @return
	 */
	public String getSelfPID() {
		if(StringUtils.isNotBlank(strPId)){
			// 缓存存在则直接使用缓存中的值
			return strPId;
		}
		// runtimeMXBean.getName()取得的值包括两个部分：PID和hostname，两者用@连接。
		String name = ManagementFactory.getRuntimeMXBean().getName();
		String pid = name.split("@")[0];
		// 值添加到缓存
		strPId = pid;

		return pid;

	}

	private String localip = null;

	/**
	 * 多IP处理，可以得到最终ip
	 * @return
	 */
	public String getIp() {
		if(!StringUtils.isBlank(localip)){
			return localip;
		}
 		try {
			Enumeration<NetworkInterface> netInterfaces = NetworkInterface
					.getNetworkInterfaces();
			InetAddress ip = null;
			boolean finded = false;// 是否找到外网IP
			while (netInterfaces.hasMoreElements() && !finded) {
				NetworkInterface ni = netInterfaces.nextElement();
				Enumeration<InetAddress> address = ni.getInetAddresses();
				while (address.hasMoreElements()) {
					ip = address.nextElement();
					if (ip.isSiteLocalAddress()
							&& !ip.isLoopbackAddress()
							&& ip.getHostAddress().indexOf(":") == -1) {// 内网IP
						localip = ip.getHostAddress();
						break;
					}
				}
				if(localip!=null){
					break;
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
		}
		return localip;
		 
	}

	/**
	 * 进程名称本地缓存
	 */
	private String strPName = "";

	/**
	 * 获取当前进程名称
	 * @return
	 */
	public String getProcessName()
	{

		if(!StringUtils.isEmpty(strPName)){
			return strPName;
		}

		String fileName = System.getProperty("java.class.path");
		if(fileName.contains(File.separator)){
			fileName = fileName.substring(fileName.lastIndexOf(File.separator) + 1);
		}
		if(fileName.endsWith(".jar")){
			fileName = fileName.replace(".jar", "");
		}
		strPName = fileName;
		return strPName;
	}

	/**
	 * 获取默认输出编码
	 * @return
	 */
	public String getDefaultCharSet() {
		OutputStreamWriter writer = new OutputStreamWriter(new ByteArrayOutputStream());
		String enc = writer.getEncoding();
		try {
			writer.close();
		} catch (IOException e) {
			logger.error("获取输出流编码失败:" + e.getMessage(), e);
		}
		return enc;
	}

}

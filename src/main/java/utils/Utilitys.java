package utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utilitys {

	public static void sleep(long lMillis) {
		try {
			Thread.sleep(lMillis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void write(File f, String strContent) throws IOException {
		if (f == null) {
			return;
		}
		if (strContent == null) {
			return;
		}
		if (f.exists() == false) {
			return;
		}
		FileChannel out = null;
		try {
			out = new FileOutputStream(f).getChannel();
			byte[] byData = strContent.getBytes();
			ByteBuffer buffer = ByteBuffer.allocate(1024);
			out.position(0);
			buffer.put(byData);
			buffer.flip();
			out.write(buffer);
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static boolean isNil(String str) {
		if (str == null || "".equals(str.trim())) {
			return true;
		}
		return false;
	}

	public static void fileAppend(String fileName, String content) {
		FileWriter writer = null;
		try {
			// 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
			writer = new FileWriter(fileName, true);
			writer.write(content);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {

					e.printStackTrace();
				}
			}
		}
	}

	public static String getStrNow() {
		Date dtNow = new Date();
		return getFormatDateTime(dtNow,"yyyy-MM-dd HH:mm:ss");
	}

	public static String getStrNowDate() {
 		Date dtNow = new Date();
		return getFormatDateTime(dtNow,"yyyy-MM-dd");
	}
	
	public static String getStrLastDate() {
		Calendar c = Calendar.getInstance();
		c.setTime(new Date());
		c.add(c.DATE, -1);
		return getFormatDateTime(c.getTime(),"yyyy-MM-dd");
	}
	
	public static String getFormatDateTime(Date date, String strFormat){
		SimpleDateFormat sdf = new SimpleDateFormat(strFormat);
		return sdf.format(date);
	}

	public static String getArgValue(String[] args, String strArgName,
                                     String strDefaultValue) {
		if (args == null || args.length == 0) {
			return strDefaultValue;
		}
		for (int i = 0; i < args.length; i++) {
			String strArg = args[i];
			if (strArg.startsWith("--" + strArgName)) {
				String[] strArgSplit = strArg.split("=");
				if (strArgSplit.length == 2) {
					return strArgSplit[1];
				}
			}
		}
		return strDefaultValue;
	}
	
	public static boolean  existsArg(String[] args, String strArgName){
		if (args == null || args.length == 0) {
			return false;
		}
		for (int i = 0; i < args.length; i++) {
			String strArg = args[i];
			if (strArg.startsWith("--" + strArgName)) {
				return true;
			}
		}
		return false;
	}

	public static Properties getProp(String[] args, String strArgName,
                                     String strDefaultPropFile) throws IOException {
		return getProp(args, strArgName, strDefaultPropFile, Utilitys.class);
	}

	public static Properties getProp(String[] args, String strArgName,
                                     String strDefaultPropFile, Class clasz) throws IOException {
		InputStream is = null;
		if (args != null && args.length > 0) {
			String configFile = Utilitys.getArgValue(args, strArgName, "");
			if (StringUtils.isEmpty(configFile) == false) {
				is = new FileInputStream(new File(configFile));
			}
		}
		if (is == null) {
			is = clasz.getResourceAsStream(strDefaultPropFile);
		}

		Properties p = new Properties();
		p.load(is);
		is.close();
		return p;
	}
	
	public static Properties getProp(String strPropFile)throws IOException {
		return getProp(strPropFile, Utilitys.class);
	}
	
	public static Properties getProp(String strPropFile, Class clasz)throws IOException {
		InputStream is = clasz.getResourceAsStream(strPropFile);
		Properties p = new Properties();
		p.load(is);
		is.close();
		return p;
		
	}

	/**
	 * 获取json配置文件中的内容
	 * @param strJSONFilePath
	 * @return
	 * @throws IOException
	 */
	public static String getJSONFileData(String strJSONFilePath)throws IOException {
		InputStream is = Utilitys.class.getResourceAsStream(strJSONFilePath);
		String strJSONData = IOUtils.toString(is);
		return strJSONData;

	}


	public static boolean isVar(String strVar) {
		if (strVar == null) {
			return false;
		}
		if (strVar.startsWith("{") && strVar.endsWith("}")) {
			return true;
		} else {
			return false;
		}
	}

	public static String getVarName(String strVar) {
		if (isVar(strVar) == false) {
			return "";
		}
		int nEndIndex = strVar.lastIndexOf("}");
		String strVarName = strVar.substring(1, nEndIndex);
		return strVarName;
	}
	
	public static String[] getDiffDataKey(List<Map<String,Object>> diffData){
	     String[]strDiffDatakeyArr=new String[diffData.size()];
		 for (int i = 0; i <diffData.size(); i++) {
			  Map<String,Object> colData=diffData.get(i);
			  strDiffDatakeyArr[i]=colData.get("strName").toString();
		 }
		 return strDiffDatakeyArr;
	}
	
	public static String getValueByKey(List<Map<String,Object>> diffData, String strKey){
	     String value = "";
		 for (int i = 0; i <diffData.size(); i++) {
			  Map<String,Object> colData=diffData.get(i);
			  String strName=colData.get("strName").toString();
			  if(strName.equals(strKey)){
				  value = colData.get("strValue").toString();
			  }
		 }
		 return value;
	}

	public static void main(String[] args) {
		System.out.println(getStrLastDate());
	}
}

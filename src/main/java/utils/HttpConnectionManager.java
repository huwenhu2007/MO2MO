package utils;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class HttpConnectionManager {
	Logger logger = Logger.getLogger(HttpConnectionManager.class);
	
	// 单例开始
	private HttpConnectionManager(){}
	
	private static HttpConnectionManager httpConnectionManager = new HttpConnectionManager();
	
	public static HttpConnectionManager getInstance(){
		if(httpClient == null){
			synchronized (httpConnectionManager) {
				if(httpClient == null){
					try {
						LayeredConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(createIgnoreVerifySSL());
				        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory> create()
				                .register("https", sslsf)
				                .register("http", new PlainConnectionSocketFactory())
				                .build();
				        cm =new PoolingHttpClientConnectionManager(socketFactoryRegistry);
				        cm.setMaxTotal(100);
				        cm.setDefaultMaxPerRoute(100);
				        
				        httpClient = HttpClients.custom()
				                				.setConnectionManager(cm)
				                				.build();     
				        
					} catch (Exception e) {
						throw new RuntimeException("httpclient连接池建立失败：" + e.getMessage(), e);
					}
				}
			}
		}
		return httpConnectionManager;
	}
	
	public static SSLContext createIgnoreVerifySSL() throws NoSuchAlgorithmException, KeyManagementException {
	    SSLContext sc = SSLContext.getInstance("SSLv3");
	  
	    // 实现一个X509TrustManager接口，用于绕过验证，不用修改里面的方法  
	    X509TrustManager trustManager = new X509TrustManager() {
	        @Override
	        public void checkClientTrusted(  
	                java.security.cert.X509Certificate[] paramArrayOfX509Certificate,  
	                String paramString) throws CertificateException {
	        }  
	  
	        @Override
	        public void checkServerTrusted(  
	                java.security.cert.X509Certificate[] paramArrayOfX509Certificate,  
	                String paramString) throws CertificateException {
	        }  
	  
	        @Override
	        public java.security.cert.X509Certificate[] getAcceptedIssuers() {  
	            return null;  
	        }  
	    };  
	  
	    sc.init(null, new TrustManager[] { trustManager }, null);
	    return sc;  
	} 
	
	private static PoolingHttpClientConnectionManager cm = null;
	private static CloseableHttpClient httpClient = null;
	
	public String postForText(String strUrl, String strBody){
		long start = System.currentTimeMillis();
		// 开始进行post请求
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		String strReturn = "";
		try {
			HttpPost post = new HttpPost(strUrl);
			if(strBody != null){
				StringEntity entityBody = new StringEntity(strBody.toString(), "utf-8");// 解决中文乱码问题
				entityBody.setContentEncoding("UTF-8");
				entityBody.setContentType("application/json");
				post.setEntity(entityBody);
			}
			
			//连接超时时间
			int connTimeOut = 1000*60;
			int connManagerTimeOut = 1000*60;
			int socketTimeOut = 1000*60;
			//配置超时时间
			RequestConfig requestConfig = RequestConfig.custom()    
			        .setConnectTimeout(connTimeOut).setConnectionRequestTimeout(connManagerTimeOut)    
			        .setSocketTimeout(socketTimeOut).build();
			post.setConfig(requestConfig);
			response = httpClient.execute(post);
			entity = response.getEntity();
			if(entity != null){
				strReturn = EntityUtils.toString(entity , "UTF-8").trim(); 
			}
			long end = System.currentTimeMillis();
			logger.info("HttpClient POST for:" + strUrl + " 参数：【" + strBody + "】请求花费时间：" + (end - start) + "ms" + 
			" poolstatus:" + HttpConnectionManager.getInstance().getPoolState());
			return strReturn;
		} catch (Exception e) {
			logger.error("httpclient Exception:" + e.getMessage(), e);
			return "";
		} finally {
			if(entity != null){
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					logger.error("entity inputstream 连接关闭 IOException:" + e.getMessage(), e);
				}
			}
			if(response != null){
				try {
					response.close();
				} catch (IOException e) {
					logger.error("response 连接关闭 IOException:" + e.getMessage(), e);
				}
			}
		}
	}
	
	public String getPoolState(){
		return cm.getTotalStats().toString();
	}
	

	
	public static void main(String[] args) {

		
	}
	
}

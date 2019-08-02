package cuckoo;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.TimeUnit;

/**
 * 异常缓存，相同的异常信息在一小时内不会重复发送
 * @Author huwenhu
 * @Date 2019/8/2 14:11
 **/
public class ExceptionCache {

    private ExceptionCache(){}

    private static  ExceptionCache exceptionCache = new ExceptionCache();

    public static ExceptionCache getInstance(){
        return exceptionCache;
    }

    /**
     * 缓存不存在时返回的默认值
     * @return
     */
    private CacheLoader<String, Boolean> createCacheLoader() {
        return new CacheLoader<String, Boolean>() {
            @Override
            public Boolean load(String key) throws Exception {
                return false;
            }
        };
    }

    /**
     * 通知异常缓存
     */
    private LoadingCache<String, Boolean> cache = CacheBuilder.newBuilder()
            .expireAfterWrite(60, TimeUnit.MINUTES)
            .build(createCacheLoader());

    /**
     * 异常是否存在
     * @param strKey
     * @return
     * @throws Exception
     */
    public boolean isExist(String strKey) throws Exception{
        return cache.get(strKey);
    }

    /**
     * 添加数据到缓存
     * @param strKey
     * @param bFlag
     */
    public void put(String strKey, Boolean bFlag){
        cache.put(strKey, bFlag);
    }

}

package oplog.listener.kafka.impl;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import log.LogConfiguration;
import oplog.entity.DMLEvent;
import oplog.listener.DMLListenerAbs;
import oplog.listener.kafka.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * 向目标kafka中存入数据
 *   库名为topic,通过表明hash与partition总数取模，保证同一个库中的表数据都能落到一个partition中，保证表数据能顺序读取
 * @Author huwenhu
 * @Date 2019/7/30 16:47
 **/
public class DMLKafkaListener extends DMLListenerAbs {

    private Logger logger  = Logger.getLogger(DMLKafkaListener.class);

    /**
     * kafka配置信息
     */
    private KafkaConfig kafkaConfig;

    /**
     * 生产者对象
     */
    private KafkaProducer<String, String> producer;

    @Override
    public void init(String strWorkSign, String strSign, JSONObject jsonObject, boolean isDebug) throws Exception {
        // kafka配置信息对象
        Properties props = new Properties();
        props.put("client.id", new StringBuilder(strWorkSign).append("-").append(strSign).toString());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        // 获取kafka参数配置信息
        JSONObject jsonParamObject = jsonObject.getJSONObject("jsonParamObject");
        Set<String> setKey = jsonParamObject.keySet();
        Iterator<String> iterator = setKey.iterator();
        while(iterator.hasNext()){
            String key = iterator.next();
            Object value = jsonParamObject.get(key);
            props.put(key, value);
        }
        // 库表转换规则
        JSONObject jsonDBRule = jsonObject.getJSONObject("jsonDBRule");
        JSONObject jsonTableRule = jsonObject.getJSONObject("jsonTableRule");

        kafkaConfig = new KafkaConfig(props, strWorkSign, strSign, isDebug, jsonDBRule, jsonTableRule);

        logger.info(String.format("%s-%s target kafka init %n%s", kafkaConfig.getStrWorkSign(), kafkaConfig.getStrSign(), kafkaConfig.toString()));
    }

    @Override
    public void start() throws Exception {
        producer = new KafkaProducer<String, String>(kafkaConfig.getProps());
        logger.info(String.format("%s-%s target kafka started", kafkaConfig.getStrWorkSign(), kafkaConfig.getStrSign()));
    }

    @Override
    public void onEvent(DMLEvent event) throws Exception {

        if(kafkaConfig.isDebug()){
            logger.error(String.format("%s-%s 目标处理事件信息:%s", kafkaConfig.getStrWorkSign(), kafkaConfig.getStrSign(), event.toString()));
        }

        // 获取事件中的库表信息
        JSONObject jsonObject = ruleChangeName(event, kafkaConfig.getJsonDBRule(), kafkaConfig.getJsonTableRule());
        if(jsonObject.isEmpty()){
            logger.error(String.format("%s-%s 事件中不存在库表信息", kafkaConfig.getStrWorkSign(), kafkaConfig.getStrSign()));
            return;
        }
        // 获取需要操作的库表名称
        String strDBName = jsonObject.getString("strDBName");
        String strCollectionName = jsonObject.getString("strCollectionName");
        // 按照事件调用对应的方法
        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(strDBName, strCollectionName, event.toJSONString()));
        RecordMetadata recordMetadata = future.get();
        if(!recordMetadata.hasOffset()){
            throw new RuntimeException(String.format("%s 目标事件处理失败 result:%s",event.toString(), recordMetadata.toString()));
        }
    }

    @Override
    public void destroy() {
        producer.flush();
        producer.close();
    }

    public static void main(String[] args){
        LogConfiguration.initLog("kafka");

        BasicDBObject queryObj = null;

        BasicDBObject updObj = new BasicDBObject();
        updObj.put("age", 123);
        updObj.put("message", "asdjhh");

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("db", "account");
        jsonObject.put("collection", "tbTrade");
        jsonObject.put("op", "i");
        jsonObject.put("q", queryObj);
        jsonObject.put("u", updObj);
        String _j = jsonObject.toString();
        System.out.println(_j);

        Properties props = new Properties();
        props.put("client.id", "asddsa");
        props.put("bootstrap.servers", "");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("account", "tbTrade", _j));
        try {
            RecordMetadata recordMetadata = future.get();
            if(recordMetadata.hasOffset()){
                System.out.println("数据发送成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.flush();
        producer.close();

    }

}

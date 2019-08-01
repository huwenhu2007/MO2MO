package oplog.listener.kafka;

import com.alibaba.fastjson.JSONObject;

import java.util.Properties;

/**
 * kafka配置信息对象
 * @Author huwenhu
 * @Date 2019/8/1 9:41
 **/
public class KafkaConfig {
    /**
     * 参数信息
     */
    private Properties props;
    /**
     * 数据库转换规则
     */
    private JSONObject jsonDBRule;
    /**
     * 表转换规则
     */
    private JSONObject jsonTableRule;


    private String strWorkSign;
    private String strSign;
    private boolean isDebug;

    public KafkaConfig(Properties props, String strWorkSign, String strSign, boolean isDebug,
                       JSONObject jsonDBRule, JSONObject jsonTableRule) {
        this.props = props;
        this.strWorkSign = strWorkSign;
        this.strSign = strSign;
        this.isDebug = isDebug;
        this.jsonDBRule = jsonDBRule;
        this.jsonTableRule = jsonTableRule;
    }

    public Properties getProps() {
        return props;
    }

    public String getStrWorkSign() {
        return strWorkSign;
    }

    public String getStrSign() {
        return strSign;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public JSONObject getJsonDBRule() {
        return jsonDBRule;
    }

    public JSONObject getJsonTableRule() {
        return jsonTableRule;
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("param:").append(props.toString()).append("\n");
        sb.append("jsonDBRule:").append(jsonDBRule == null ? "" : jsonDBRule.toString()).append("\n");
        sb.append("jsonTableRule:").append(jsonTableRule == null ? "" : jsonTableRule.toString());
        return sb.toString();
    }
}

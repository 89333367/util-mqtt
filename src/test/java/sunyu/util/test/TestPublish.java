package sunyu.util.test;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttPublishUtil;
import sunyu.util.mqtt.QosLevel;

/**
 * 发送消息测试：向 {@code sy/bcld/report} 发送多条消息，由订阅端（TestSubscribe1）接收。
 *
 * <p>典型流程：
 * <ol>
 *   <li>构造 {@link MqttPublishUtil}（cleanSession=true，会话不保留）</li>
 *   <li>循环调用 {@link MqttPublishUtil#publish(String, QosLevel, String)} 同步发送</li>
 *   <li>publish() 方法返回即代表消息已被 broker 确认（拿到 PUBACK/PUBCOMP）</li>
 * </ol>
 *
 * @author SunYu
 */
public class TestPublish {
    static final Log log = LogFactory.get();
    static final String PUB_TOPIC = "sy/bcld/report";
    static final int COUNT = 10;

    @Test
    void t001() {
        MqttPublishUtil producer = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-producer-001")  // 发送端的 clientId，与消费端不同
                .build();

        for (int i = 0; i < COUNT; i++) {
            String payload = "msg-" + DateTime.now();
            producer.publish(PUB_TOPIC, QosLevel.AT_LEAST_ONCE, payload);
            log.info("[发送成功] topic={}, payload={}", PUB_TOPIC, payload);
            ThreadUtil.sleep(1000 * 2);
        }

        log.info("全部消息已发送并确认");
        producer.close();
    }
}

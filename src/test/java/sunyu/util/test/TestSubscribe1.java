package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttPublishUtil;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

/**
 * 正常消费演示：订阅消息 → 业务处理 → 自动 ACK。
 *
 * <p>使用推荐的 setMessageHandler 方式：订阅端只负责"收消息"。
 * 如果需要下发指令或把失败消息重新发布，请使用独立的 {@link MqttPublishUtil}。
 *
 * @author SunYu
 */
public class TestSubscribe1 {
    static final Log log = LogFactory.get();
    static final String SUB_TOPIC = "$share/group1/" + TestPublish.PUB_TOPIC;

    @Test
    void t001() throws InterruptedException {
        // 1) 创建一个独立的发布端，专门用于"处理中下发指令 / 失败重发"
        try (MqttPublishUtil publisher = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-publisher-001")
                .build();

             // 2) 创建订阅端，仅负责"收消息"
             MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-001")
                .setMessageHandler((topic, message) -> {
                    log.info("[收到消息-1] messageId={}, topic={}, payload={}",
                            message.getId(), topic, new String(message.getPayload()));

                    // 业务处理：入库、转换、过滤等
                    // ...

                    // 处理完毕后需要发送消息时，使用独立的 publisher（而非 consumer）
                    publisher.publish("command/did",
                                      QosLevel.AT_LEAST_ONCE,
                                      "发给其他监听者");
                })
                .build()) {

            consumer.subscribe(SUB_TOPIC, QosLevel.AT_LEAST_ONCE);

            log.info("TestSubscribe1 启动完成，持续消费...");
            Thread.currentThread().join();
        }
    }
}

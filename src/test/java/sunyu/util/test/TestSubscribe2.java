package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttPublishUtil;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

/**
 * 失败重试演示：业务处理失败 → 使用独立的 MqttPublishUtil 把消息重新发布回原主题 →
 * broker 按共享订阅规则重新负载均衡到同组的某个订阅者。
 *
 * @author SunYu
 */
public class TestSubscribe2 {
    static final Log log = LogFactory.get();
    static final String SUB_TOPIC = "$share/group1/" + TestPublish.PUB_TOPIC;

    @Test
    void t001() throws InterruptedException {
        // 1) 创建一个独立的发布端，专门用于"失败后重新发布回原主题"
        try (MqttPublishUtil publisher = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-publisher-002")
                .build();

             // 2) 创建订阅端，仅负责"收消息"
             MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-002")
                .setMessageHandler((topic, message) -> {
                    try {
                        log.info("[收到消息-2] messageId={}, topic={}, payload={}",
                                message.getId(), topic, new String(message.getPayload()));

                        // 模拟业务失败（例如数据库连接异常）
                        String payload = new String(message.getPayload());
                        if ("bad".equals(payload)) {
                            throw new RuntimeException("模拟业务处理失败");
                        }
                        // 正常处理完毕，Paho 协议层已自动 ACK，无需额外操作
                    } catch (Exception e) {
                        log.warn("[处理失败-2] {} → 使用独立的 MqttPublishUtil 把消息重新发布回原主题",
                                e.getMessage());
                        // 关键：使用独立的 publisher，不要在订阅端连接上做发布
                        publisher.publish(topic,
                                          QosLevel.AT_LEAST_ONCE,
                                          message.getPayload());
                    }
                })
                // 可选：监听连接断开事件
                .setDisconnectedHandler(response -> log.warn("[连接断开] {}",
                        response != null ? response.getReasonString() : "null"))
                .build()) {

            consumer.subscribe(SUB_TOPIC, QosLevel.AT_LEAST_ONCE);

            log.info("TestSubscribe2 启动完成（业务失败后会重新发布到原主题）...");
            Thread.currentThread().join();
        }
    }
}

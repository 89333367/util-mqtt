package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttPublishUtil;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 正常消费演示：订阅消息 → 业务处理 → 自动 ACK。
 *
 * <p>使用推荐的 setMessageHandler 方式：订阅端只负责"收消息"。
 * 如果需要下发指令或把失败消息重新发布，请使用独立的 {@link MqttPublishUtil}。
 *
 * <p>t001 演示字符串消息的基本收发。
 * <p>t002 演示 MQTT 5 {@link MqttProperties}（消息级元数据：userProperty、contentType、
 * responseTopic、correlationData、messageExpiryInterval 等）的发送与消费。
 *
 * @author SunYu
 */
public class TestSubscribe1 {
    static final Log log = LogFactory.get();
    static final String SUB_TOPIC = "$share/group1/" + TestPublish.PUB_TOPIC;
    static final String SUB_PROPS_TOPIC = "$share/group1/" + TestPublish.PUB_PROPS_TOPIC;

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
                         // 发布端以 UTF-8 编码字符串为字节数组，消费端必须显式用 UTF-8 还原，
                         // 避免不同操作系统 / JVM 的默认编码差异造成乱码（尤其 Windows）
                         log.info("[收到消息-1] messageId={}, topic={}, payload={}",
                                 message.getId(), topic,
                                 new String(message.getPayload(), StandardCharsets.UTF_8));

                         // 业务处理：入库、转换、过滤等
                         // ...

                         // 处理完毕后需要发送消息时，使用独立的 publisher（而非 consumer）
                         publisher.publish("command/did",
                                 QosLevel.AT_LEAST_ONCE,
                                 "发给其他监听者");
                     })
                     .build()) {

            consumer.subscribe(SUB_TOPIC, QosLevel.AT_LEAST_ONCE);

            log.info("TestSubscribe1 t001 启动完成，持续消费...");
            Thread.currentThread().join();
        }
    }

    /**
     * 消费 MQTT 5 的 {@link MqttProperties}：演示如何从收到的消息中读取
     * userProperty、contentType、responseTopic、correlationData、messageExpiryInterval 等属性。
     *
     * <p>配合 {@link TestPublish#t002()} 使用。
     */
    @Test
    void t002() throws InterruptedException {
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-props-001")
                .setMessageHandler((topic, message) -> {
                    // 1) payload：与发布端约定使用 UTF-8 编码
                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    log.info("[收到消息-props] messageId={}, topic={}, qos={}, payload={}",
                            message.getId(), topic, message.getQos(), payload);

                    // 2) 读取 MQTT 5 协议级属性（核心演示点）
                    MqttProperties props = message.getProperties();
                    if (props != null) {
                        // contentType：通常用来描述 payload 的 MIME 类型，例如 "application/json"
                        log.info("  · contentType={}", props.getContentType());

                        // responseTopic + correlationData：典型"请求/响应"模式的
                        // MQTT 5 原生方式，避免把这些元数据塞进 payload
                        log.info("  · responseTopic={}", props.getResponseTopic());
                        byte[] corr = props.getCorrelationData();
                        log.info("  · correlationData={}",
                                corr == null ? null : new String(corr, StandardCharsets.UTF_8));

                        // messageExpiryInterval：broker 会在该秒数内丢弃该未投递消息
                        log.info("  · messageExpiryInterval(秒)={}", props.getMessageExpiryInterval());

                        // payloadFormat：true 表示"UTF-8 字符串 / UTF-8 JSON 等文本"，
                        // false 表示"二进制数据"。仅作提示，不强制校验
                        log.info("  · payloadFormat(UTF-8标志)={}", props.getPayloadFormat());

                        // userProperties：自定义键值对（可多个）——用于业务参数、租户 ID、
                        // 链路追踪 traceId 等，完全不污染 payload
                        List<UserProperty> ups = props.getUserProperties();
                        if (ups != null) {
                            for (UserProperty up : ups) {
                                log.info("  · userProperty {}={}", up.getKey(), up.getValue());
                            }
                        }
                    } else {
                        log.info("  · 该消息未携带 MqttProperties");
                    }
                })
                .build()) {

            consumer.subscribe(SUB_PROPS_TOPIC, QosLevel.AT_LEAST_ONCE);

            log.info("TestSubscribe1 t002 启动完成，持续消费（MqttProperties 演示）...");
            Thread.currentThread().join();
        }
    }
}

package sunyu.util.test;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttPublishUtil;
import sunyu.util.mqtt.QosLevel;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 发送消息测试：向 {@code sy/bcld/report} 发送多条消息，由订阅端（TestSubscribe1）接收。
 *
 * <p>典型流程：
 * <ol>
 *   <li>构造 {@link MqttPublishUtil}（cleanStart=true，会话不保留）</li>
 *   <li>循环调用 {@link MqttPublishUtil#publish(String, QosLevel, String)} 同步发送</li>
 *   <li>publish() 方法返回即代表消息已被 broker 确认（拿到 PUBACK/PUBCOMP）</li>
 * </ol>
 *
 * <p>t001：字符串消息的基本收发。
 * <p>t002：MQTT 5 {@link MqttProperties}（消息级元数据）发送演示，配合
 * {@link TestSubscribe1#t002()} 观察订阅端如何读取属性。
 *
 * @author SunYu
 */
public class TestPublish {
    static final Log log = LogFactory.get();
    static final String PUB_TOPIC = "sy/bcld/report";
    static final String PUB_PROPS_TOPIC = "sy/bcld/report/props";
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

    /**
     * MQTT 5 {@link MqttProperties} 发送演示：通过协议级属性附加元数据，不污染 payload。
     *
     * <p>典型用法：
     * <ul>
     *   <li>{@code userProperty}：自定义 k/v（业务参数、traceId、租户 ID 等），可多个</li>
     *   <li>{@code contentType}：payload 的 MIME 类型（例如 {@code application/json}）</li>
     *   <li>{@code responseTopic} + {@code correlationData}：
     *       MQTT 5 原生的"请求/响应"模式，响应方直接回写到 responseTopic，
     *       调用方通过 correlationData 匹配请求与响应</li>
     *   <li>{@code messageExpiryInterval}：broker 保留该消息的最长秒数（TTL），
     *       超过后 broker 自动丢弃，避免堆积旧消息</li>
     *   <li>{@code payloadFormat}：true 表示 UTF-8 文本，false 表示二进制</li>
     * </ul>
     */
    @Test
    void t002() {
        try (MqttPublishUtil producer = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-producer-props-001")
                .build()) {

            for (int i = 0; i < COUNT; i++) {
                // 1) payload：UTF-8 JSON（与 contentType 呼应）
                String payload = "{\"index\":" + i
                        + ",\"time\":\"" + DateTime.now() + "\"}";

                // 2) 组装 MQTT 5 消息属性（核心演示点）
                MqttProperties props = new MqttProperties();

                // payload 格式提示：true 表示 UTF-8 文本
                props.setPayloadFormat(true);
                // payload 的 MIME 类型
                props.setContentType("application/json");
                // 消息过期时间（秒）：broker 超过此时间仍未投递则丢弃
                props.setMessageExpiryInterval(600L);
                // "请求/响应"模式：响应方应把响应发到此主题
                props.setResponseTopic("sy/bcld/response");
                // 与响应方匹配用的关联数据（通常是 UUID 或请求 ID 的字节数组）
                props.setCorrelationData(("req-" + i + "-" + System.nanoTime())
                        .getBytes(StandardCharsets.UTF_8));
                // 自定义属性：可多个，典型用于 traceId / tenantId / version 等
                props.setUserProperties(Arrays.asList(
                        new UserProperty("traceId", "t-" + System.nanoTime()),
                        new UserProperty("version", "v2-mqtt5"),
                        new UserProperty("producer", "demo-producer-props-001")
                ));

                // 3) 发布：四参重载方法，payload + properties 各自独立
                producer.publish(PUB_PROPS_TOPIC, QosLevel.AT_LEAST_ONCE,
                        payload.getBytes(StandardCharsets.UTF_8), props);
                log.info("[发送成功-props] topic={}, payload={}", PUB_PROPS_TOPIC, payload);

                ThreadUtil.sleep(1000 * 2);
            }
        }

        log.info("props 演示消息全部发送并确认");
    }
}

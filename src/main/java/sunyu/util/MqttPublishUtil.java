package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import sunyu.util.mqtt.QosLevel;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTT <b>消息生产者</b>工具类 —— 基于同步客户端 {@link MqttClient}，专用于发送消息。
 *
 * <h3>关键配置</h3>
 * <ul>
 *   <li>{@code cleanSession = true}（硬编码）：发送端是"即用即走"模式，broker 不需要记住本客户端状态。</li>
 *   <li>{@code automaticReconnect = true}：底层网络断开时自动重连。</li>
 *   <li>{@code MemoryPersistence}：本地缓存未完成的 QoS 1/2 消息，重连后自动重试。</li>
 *   <li>{@code publish()} 是同步阻塞的：方法返回即代表消息已被 broker 确认（QoS 1 拿到 PUBACK，QoS 2 拿到 PUBCOMP）。</li>
 * </ul>
 *
 * <h3>典型用法</h3>
 * <pre>
 * try (MqttPublishUtil producer = MqttPublishUtil.builder()
 *         .setBroker("tcp://broker:1883")
 *         .setClientId("my-service-producer")
 *         .setUsername("user")
 *         .setPassword("pwd")
 *         .build()) {
 *
 *     producer.publish("sy/bcld/command/device-123", QosLevel.AT_LEAST_ONCE, "payload");
 *     // publish() 返回即代表 broker 已确认
 * }
 * </pre>
 *
 * @author SunYu
 */
public class MqttPublishUtil implements AutoCloseable {

    private static final Log log = LogFactory.get();

    private final MqttClient client;
    private final String broker;
    private final String clientId;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private MqttPublishUtil(Config config) {
        this.broker = config.broker;
        this.clientId = config.clientId;

        try {
            client = new MqttClient(config.broker, config.clientId, new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(config.automaticReconnect);
            options.setConnectionTimeout(config.connectionTimeoutSeconds);
            options.setKeepAliveInterval(config.keepAliveIntervalSeconds);
            if (config.username != null) options.setUserName(config.username);
            if (config.password != null) options.setPassword(config.password);

            log.info("[MqttPublishUtil] 开始连接 broker={} clientId={} cleanSession=true", broker, clientId);
            client.connect(options);
            log.info("[MqttPublishUtil] 连接成功 clientId={}", clientId);
        } catch (MqttException e) {
            log.error("[MqttPublishUtil] 初始化失败 broker={} clientId={} reasonCode={} {}",
                    broker, clientId, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("MqttPublishUtil 初始化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 同步发送一条消息。方法阻塞直到 broker 确认或超时/失败抛异常。
     *
     * @param topic 目标主题
     * @param qos   QoS 等级（推荐 AT_LEAST_ONCE）
     * @param msg   消息内容（字符串，按 UTF-8 编码）
     * @throws IllegalArgumentException 参数非法
     * @throws RuntimeException         发送失败（包装 {@link MqttException}）
     */
    public void publish(String topic, QosLevel qos, String msg) {
        if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("topic 不能为空");
        if (qos == null) throw new IllegalArgumentException("qos 不能为 null");
        if (msg == null) throw new IllegalArgumentException("msg 不能为 null");

        MqttMessage message = new MqttMessage(msg.getBytes(StandardCharsets.UTF_8));
        message.setQos(qos.value());

        try {
            client.publish(topic, message);
            log.debug("[MqttPublishUtil] publish 完成 topic={} qos={} payloadLen={}",
                    topic, qos, msg.length());
        } catch (MqttException e) {
            log.error("[MqttPublishUtil] publish 异常 topic={} qos={} reasonCode={} {}",
                    topic, qos, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("publish 失败: " + e.getMessage(), e);
        }
    }

    public String getBroker() {
        return broker;
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        log.info("[MqttPublishUtil] 关闭 clientId={}", clientId);
        try {
            if (client.isConnected()) client.disconnect(10_000);
        } catch (MqttException e) {
            log.warn("[MqttPublishUtil] disconnect 异常 {}", e.getMessage());
            try {
                client.disconnectForcibly(5000, 10_000);
            } catch (MqttException ex) {
                log.warn("[MqttPublishUtil] 强制断开也失败 {}", ex.getMessage());
            }
        }
        try {
            client.close();
        } catch (MqttException e) {
            log.warn("[MqttPublishUtil] close 异常 {}", e.getMessage());
        }
        log.info("[MqttPublishUtil] 关闭完成 clientId={}", clientId);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static class Config {
        String broker = "tcp://broker.emqx.io:1883";
        String clientId;
        String username;
        char[] password;
        boolean automaticReconnect = true;
        int connectionTimeoutSeconds = 30;
        int keepAliveIntervalSeconds = 60;
    }

    public static class Builder {
        private final Config config = new Config();

        public Builder setBroker(String broker) {
            config.broker = broker;
            return this;
        }

        public Builder setClientId(String clientId) {
            config.clientId = clientId;
            return this;
        }

        public Builder setUsername(String username) {
            config.username = username;
            return this;
        }

        public Builder setPassword(char[] password) {
            config.password = password;
            return this;
        }

        public Builder setPassword(String password) {
            config.password = password == null ? null : password.toCharArray();
            return this;
        }

        public Builder setAutomaticReconnect(boolean automaticReconnect) {
            config.automaticReconnect = automaticReconnect;
            return this;
        }

        public Builder setConnectionTimeoutSeconds(int seconds) {
            config.connectionTimeoutSeconds = seconds;
            return this;
        }

        public Builder setKeepAliveIntervalSeconds(int seconds) {
            config.keepAliveIntervalSeconds = seconds;
            return this;
        }

        public MqttPublishUtil build() {
            if (config.clientId == null || config.clientId.isEmpty())
                throw new IllegalArgumentException("clientId 不能为空");
            return new MqttPublishUtil(config);
        }
    }
}

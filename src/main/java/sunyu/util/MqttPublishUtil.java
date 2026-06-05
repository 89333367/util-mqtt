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

/**
 * MQTT <b>消息生产者</b>工具类 —— 基于同步客户端 {@link MqttClient}，专用于发送消息。
 *
 * <p><b>关键配置</b>（由 Builder 传入）：
 * <ul>
 *   <li>{@code cleanSession = true}（硬编码）：发送端是"即用即走"模式，broker 不需要记住本客户端。</li>
 *   <li>{@code automaticReconnect = true}：底层网络断开时自动指数退避重连。</li>
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
    private final String clientId;

    private MqttPublishUtil(String broker, String clientId, String username, char[] password,
                            boolean cleanSession, boolean automaticReconnect, int connectionTimeoutSeconds,
                            int keepAliveIntervalSeconds) {
        this.clientId = clientId;

        try {
            client = new MqttClient(broker, clientId, new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(cleanSession);
            options.setAutomaticReconnect(automaticReconnect);
            options.setConnectionTimeout(connectionTimeoutSeconds);
            options.setKeepAliveInterval(keepAliveIntervalSeconds);
            if (username != null) options.setUserName(username);
            if (password != null) options.setPassword(password);

            log.info("[MqttPublishUtil] 开始连接 broker={} clientId={} username={} password={} " +
                            "cleanSession={} automaticReconnect={} connectionTimeoutSec={} keepAliveIntervalSec={}",
                    broker, clientId, username, password != null ? "*****" : "null",
                    cleanSession, automaticReconnect, connectionTimeoutSeconds, keepAliveIntervalSeconds);
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

    public String getClientId() {
        return clientId;
    }

    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public void close() {
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

    /**
     * Builder：链式构造 {@link MqttPublishUtil}。
     *
     * <p>Builder 自己持有参数（不需要额外的 Config 中间类），{@link #build()} 时把参数交给主类构造函数即可。
     */
    public static class Builder {
        private String broker = "tcp://broker.emqx.io:1883";
        private String clientId;
        private String username;
        private char[] password;
        private boolean automaticReconnect = true;
        private int connectionTimeoutSeconds = 30;
        private int keepAliveIntervalSeconds = 60;
        private boolean cleanSession = true; // 发布者默认 true：即用即走，broker 不需要记住本客户端

        public Builder setBroker(String broker) {
            this.broker = broker;
            return this;
        }

        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(char[] password) {
            this.password = password;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password == null ? null : password.toCharArray();
            return this;
        }

        public Builder setAutomaticReconnect(boolean automaticReconnect) {
            this.automaticReconnect = automaticReconnect;
            return this;
        }

        public Builder setConnectionTimeoutSeconds(int seconds) {
            this.connectionTimeoutSeconds = seconds;
            return this;
        }

        public Builder setKeepAliveIntervalSeconds(int seconds) {
            this.keepAliveIntervalSeconds = seconds;
            return this;
        }

        /**
         * 设置 cleanSession。发布者默认 true：即用即走，broker 不需要记住本客户端。
         */
        public Builder setCleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }

        public MqttPublishUtil build() {
            if (clientId == null || clientId.isEmpty())
                throw new IllegalArgumentException("clientId 不能为空");
            return new MqttPublishUtil(broker, clientId, username, password,
                    cleanSession, automaticReconnect, connectionTimeoutSeconds, keepAliveIntervalSeconds);
        }
    }
}

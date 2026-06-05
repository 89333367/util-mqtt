package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import sunyu.util.mqtt.QosLevel;

import java.util.function.Consumer;
import java.util.UUID;

/**
 * MQTT 订阅消费端工具类 —— 基于同步客户端 {@link MqttClient}。
 *
 * <p><b>关键配置</b>：
 * <ul>
 *   <li>{@code cleanSession 默认 false}：broker 记住本 clientId 的订阅和未 ACK 消息；重连后继续推送。</li>
 *   <li><b>自动 ACK</b>：消息交给 handler 后由 Paho 自动确认。</li>
 *   <li>{@code automaticReconnect = true}：底层网络断开时自动指数退避重连。</li>
 *   <li>{@code clientId 默认自动生成}（"sub-" + UUID 前缀，确保 ≤ 23 字节）；
 *       如需 broker 记住会话（cleanSession=false），请显式设置固定且唯一的 clientId。</li>
 *   <li><b>消息处理器必须通过 {@link Builder#setMessageHandler(MessageHandler)} 传入</b>：
 *       第三个参数 util 就是本实例本身，可以直接调 util.republish(...)；
 *       connectionLost / deliveryComplete 可通过另外两个 setter 按需自定义。</li>
 * </ul>
 *
 * <h3>典型用法（推荐 setMessageHandler，不需要 holder）</h3>
 * <pre>
 * try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
 *         .setBroker("tcp://broker:1883")
 *         .setClientId("your-service-consumer")
 *         // 第三个参数 util 就是 consumer 自身，可以直接调 util.republish(...)
 *         .setMessageHandler((topic, message, util) -> {
 *             try {
 *                 // 业务处理：解析 payload、入库等
 *             } catch (Exception e) {
 *                 // 业务失败：把消息原封不动重新发回原主题，让 broker 重新负载均衡
 *                 util.republish(topic, message);
 *             }
 *         })
 *         .build()) {
 *
 *     consumer.subscribe("$share/group1/sy/bcld/report", QosLevel.AT_LEAST_ONCE);
 *     Thread.currentThread().join();
 * }
 * </pre>
 *
 * @author SunYu
 */
public class MqttSubscribeUtil implements AutoCloseable {

    private static final Log log = LogFactory.get();

    /** MQTT 3.1.1 规范限制 clientId 最大 23 字节；很多 broker 仍沿用此限制。 */
    private static final int MAX_CLIENT_ID_LENGTH = 23;

    private final MqttClient client;
    private final String clientId;

    /**
     * 消息处理器 —— 第三个参数就是 {@link MqttSubscribeUtil} 本身，
     * 业务异常时可以直接调用 {@code util.republish(topic, message)} 把消息重新发回原主题。
     *
     * <p>是 {@link FunctionalInterface}，可以直接写 lambda：
     * <pre>
     * builder.setMessageHandler((topic, message, util) -> { ... });
     * </pre>
     */
    @FunctionalInterface
    public interface MessageHandler {
        void handle(String topic, MqttMessage message, MqttSubscribeUtil util) throws Exception;
    }

    private MqttSubscribeUtil(Builder b) {
        this.clientId = b.clientId;

        try {
            client = new MqttClient(b.broker, clientId, new MemoryPersistence());

            // 组装 MqttCallback：以 messageHandler 为消息处理核心，
            // connectionLost / deliveryComplete 可通过 Builder 单独传入 Consumer 覆盖默认行为
            final MessageHandler h = b.messageHandler;
            final Consumer<Throwable> connLost = b.connectionLostHandler;
            final Consumer<IMqttDeliveryToken> delivDone = b.deliveryCompleteHandler;

            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    if (connLost != null) {
                        connLost.accept(cause);
                    } else {
                        log.warn("[MqttSubscribeUtil] 连接断开 clientId={} {}", clientId,
                                cause != null ? cause.getMessage() : "null");
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    if (delivDone != null) {
                        delivDone.accept(token);
                    }
                    // 没传则空实现（订阅端通常不需要）
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    h.handle(topic, message, MqttSubscribeUtil.this);
                }
            });

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(b.cleanSession);
            options.setAutomaticReconnect(b.automaticReconnect);
            options.setConnectionTimeout(b.connectionTimeoutSeconds);
            options.setKeepAliveInterval(b.keepAliveIntervalSeconds);
            if (b.username != null) options.setUserName(b.username);
            if (b.password != null) options.setPassword(b.password);

            log.info("[MqttSubscribeUtil] 开始连接 broker={} clientId={} username={} password={} " +
                            "cleanSession={} automaticReconnect={} connectionTimeoutSec={} keepAliveIntervalSec={}",
                    b.broker, clientId, b.username, b.password != null ? "*****" : "null",
                    b.cleanSession, b.automaticReconnect, b.connectionTimeoutSeconds, b.keepAliveIntervalSeconds);
            client.connect(options);
            log.info("[MqttSubscribeUtil] 连接成功 clientId={}", clientId);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] 初始化失败 broker={} clientId={} reasonCode={} {}",
                    b.broker, clientId, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("MqttSubscribeUtil 初始化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 同步订阅一个主题。方法阻塞直到 broker 返回 SUBACK，或超时/失败抛异常。
     *
     * 消息到达后会路由到调用方通过 {@link Builder#setMessageHandler(MessageHandler)} 设置的处理器。
     */
    public void subscribe(String topicFilter, QosLevel qos) {
        if (topicFilter == null || topicFilter.isEmpty()) throw new IllegalArgumentException("topicFilter 不能为空");
        if (qos == null) throw new IllegalArgumentException("qos 不能为 null");

        try {
            log.info("[MqttSubscribeUtil] 开始订阅 filter={} qos={}", topicFilter, qos);
            client.subscribe(topicFilter, qos.value());
            log.info("[MqttSubscribeUtil] 订阅成功 filter={}", topicFilter);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] subscribe 异常 filter={} qos={} reasonCode={} {}",
                    topicFilter, qos, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("subscribe 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 把消息原封不动重新发布到原主题。
     *
     * <p>业务异常时调用它 —— broker 会把这条"新消息"重新负载均衡给同组的订阅者（共享订阅场景），
     * 从而实现"失败交给别人重试"的效果。
     *
     * @param topic   消息到达时的实际主题名
     * @param message 原消息
     */
    public void republish(String topic, MqttMessage message) {
        if (message == null) throw new IllegalArgumentException("message 不能为 null");
        republish(topic, message.getPayload(), message.getQos());
    }

    /** {@link #republish(String, MqttMessage)} 的重载，直接传 payload + qos。 */
    public void republish(String topic, byte[] payload, int qos) {
        if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("topic 不能为空");
        if (payload == null) payload = new byte[0];

        try {
            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(qos);
            client.publish(topic, msg);
            log.info("[MqttSubscribeUtil] republish：消息已重新发布到原主题 topic={} qos={} payloadLen={}",
                    topic, qos, payload.length);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] republish 失败 topic={} qos={} reasonCode={} {}",
                    topic, qos, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("republish 失败: " + e.getMessage(), e);
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
        log.info("[MqttSubscribeUtil] 关闭 clientId={}", clientId);
        try {
            if (client.isConnected()) {
                client.disconnect(10_000);
            }
        } catch (MqttException e) {
            log.warn("[MqttSubscribeUtil] disconnect 异常 {}", e.getMessage());
            try {
                client.disconnectForcibly(5000, 10_000);
            } catch (MqttException ex) {
                log.warn("[MqttSubscribeUtil] 强制断开也失败 {}", ex.getMessage());
            }
        }
        try {
            client.close();
        } catch (MqttException e) {
            log.warn("[MqttSubscribeUtil] close 异常 {}", e.getMessage());
        }
        log.info("[MqttSubscribeUtil] 关闭完成 clientId={}", clientId);
    }

    /** 生成 clientId：prefix + UUID 截断，确保 ≤ MAX_CLIENT_ID_LENGTH。 */
    static String generateClientId(String prefix) {
        String base = (prefix == null ? "mqtt" : prefix) + "-" + UUID.randomUUID().toString().replace("-", "");
        return base.length() > MAX_CLIENT_ID_LENGTH ? base.substring(0, MAX_CLIENT_ID_LENGTH) : base;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder：链式构造 {@link MqttSubscribeUtil}。
     *
     * <p>消息处理入口：
     * <ul>
     *   <li>必需：{@link #setMessageHandler(MessageHandler)} — 第三个参数 util 就是本实例，可直接调 republish()</li>
     *   <li>可选：{@link #setConnectionLostHandler(Consumer)} — 自定义连接断开处理；不传用默认日志</li>
     *   <li>可选：{@link #setDeliveryCompleteHandler(Consumer)} — 自定义发送确认处理；不传则空实现</li>
     * </ul>
     */
    public static class Builder {
        String broker = "tcp://broker.emqx.io:1883";
        String clientId;
        String username;
        char[] password;
        boolean automaticReconnect = true;
        int connectionTimeoutSeconds = 30;
        int keepAliveIntervalSeconds = 60;
        boolean cleanSession = false; // 订阅者默认 false：broker 记住订阅和未 ACK 消息，重连后继续推送

        MessageHandler messageHandler;
        Consumer<Throwable> connectionLostHandler;
        Consumer<IMqttDeliveryToken> deliveryCompleteHandler;

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
         * 设置 cleanSession。订阅者默认 false：broker 记住本 clientId 的订阅和未 ACK 消息，重连后继续推送。
         */
        public Builder setCleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }

        /**
         * 设置消息处理器（推荐）。只需实现一个方法，第三个参数就是 {@link MqttSubscribeUtil} 自身。
         *
         * <pre>
         * builder.setMessageHandler((topic, message, util) -> {
         *     try {
         *         // 业务处理
         *     } catch (Exception e) {
         *         util.republish(topic, message); // 失败重试：直接调 util.republish
         *     }
         * });
         * </pre>
         *
         * <p>如果还需要自定义 connectionLost / deliveryComplete，可配合 {@link #setConnectionLostHandler(Consumer)}
         * 和 {@link #setDeliveryCompleteHandler(Consumer)} 使用。
         */
        public Builder setMessageHandler(MessageHandler handler) {
            this.messageHandler = handler;
            return this;
        }

        /**
         * 自定义连接断开回调。配合 {@link #setMessageHandler(MessageHandler)} 使用。
         * 不传则使用工具类默认日志输出。
         */
        public Builder setConnectionLostHandler(Consumer<Throwable> handler) {
            this.connectionLostHandler = handler;
            return this;
        }

        /**
         * 自定义消息发送确认回调。配合 {@link #setMessageHandler(MessageHandler)} 使用。
         * 不传则空实现（订阅端通常不需要）。
         */
        public Builder setDeliveryCompleteHandler(Consumer<IMqttDeliveryToken> handler) {
            this.deliveryCompleteHandler = handler;
            return this;
        }

        public MqttSubscribeUtil build() {
            if (clientId == null || clientId.isEmpty()) {
                clientId = generateClientId("sub");
                log.info("[MqttSubscribeUtil] 未设置 clientId，已自动生成：{}", clientId);
            } else if (clientId.length() > MAX_CLIENT_ID_LENGTH) {
                throw new IllegalArgumentException("clientId 超出最大长度 " + MAX_CLIENT_ID_LENGTH
                        + " 字节：当前长度 " + clientId.length() + "，值=\"" + clientId + "\"");
            }
            if (messageHandler == null)
                throw new IllegalArgumentException("messageHandler 不能为空：必须通过 setMessageHandler(...) 传入");
            return new MqttSubscribeUtil(this);
        }
    }
}

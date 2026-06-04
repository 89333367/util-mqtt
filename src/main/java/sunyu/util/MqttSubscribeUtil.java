package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import sunyu.util.mqtt.QosLevel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MQTT <b>订阅消费端</b>工具类 —— 基于同步客户端 {@link MqttClient}，专为订阅与消费设计。
 *
 * <h3>关键配置</h3>
 * <ul>
 *   <li>{@code cleanSession = false}（硬编码）：broker 记住此 clientId 的订阅和未 ACK 的消息；重连后继续推送，保证消息不丢。</li>
 *   <li>{@code setManualAcks(true)}（硬编码）：消息不会被自动 ACK，必须由调用方在业务成功后调用 {@link Acker#ack(int, int)} 手动 ACK。</li>
 *   <li>{@code automaticReconnect = true}：底层网络断开时自动重连。</li>
 *   <li>{@code clientId 必须固定且唯一}（配合 cleanSession=false）。</li>
 * </ul>
 *
 * <h3>消息分发策略（双路）</h3>
 * <ol>
 *   <li><b>普通主题</b>（如 {@code sy/bcld/report}）：通过 Paho 原生 per-subscription listener 分发。</li>
 *   <li><b>共享订阅</b>（如 {@code $queue/sy/bcld/report}）：broker 转发时会剥离前缀，
 *       Paho per-subscription listener 匹配不上，
 *       因此通过全局 {@link MqttCallback#messageArrived(String, MqttMessage)} 做"去前缀二次匹配"兜底。</li>
 * </ol>
 *
 * <h3>典型用法</h3>
 * <pre>
 * try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
 *         .setBroker("tcp://broker:1883")
 *         .setClientId("your-service-consumer") // 必须固定且唯一
 *         .setUsername("user")
 *         .setPassword("pwd")
 *         .build()) {
 *
 *     consumer.subscribe("$queue/sy/bcld/report", QosLevel.AT_LEAST_ONCE,
 *             (topic, message, acker) -> {
 *                 try {
 *                     // 业务处理：解析 payload、入库等
 *                     acker.ack(message.getId(), message.getQos()); // 成功才 ACK
 *                 } catch (Exception e) {
 *                     // 不 ACK，让 broker 重发
 *                 }
 *             });
 *
 *     // 持续消费，直到进程退出或 close()
 *     Thread.currentThread().join();
 * }
 * </pre>
 *
 * @author SunYu
 */
public class MqttSubscribeUtil implements AutoCloseable {

    private static final Log log = LogFactory.get();

    private final MqttClient client;
    private final String broker;
    private final String clientId;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /**
     * filter -> handler，用于全局 callback 对共享订阅做二次匹配。
     */
    private final ConcurrentHashMap<String, MqttMessageHandler> handlers = new ConcurrentHashMap<>();

    private MqttSubscribeUtil(Config config) {
        this.broker = config.broker;
        this.clientId = config.clientId;

        try {
            client = new MqttClient(config.broker, config.clientId, new MemoryPersistence());

            // 全局 MqttCallback：用于共享订阅（$queue/$share 前缀）的二次匹配分发
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    log.warn("[MqttSubscribeUtil] 连接断开 clientId={} {}", clientId, cause.getMessage());
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }

                @Override
                public void messageArrived(String actualTopic, MqttMessage message) {
                    for (Map.Entry<String, MqttMessageHandler> entry : handlers.entrySet()) {
                        String filter = entry.getKey();
                        if (!matchesFilter(filter, actualTopic)) continue;
                        MqttMessageHandler h = entry.getValue();
                        try {
                            h.onMessage(actualTopic, message, MqttSubscribeUtil.this::ack);
                        } catch (Exception e) {
                            log.warn("[MqttSubscribeUtil] handler 异常（不 ACK，让 broker 重发）" +
                                    " topic={} messageId={} {}", actualTopic, message.getId(), e.getMessage());
                        }
                    }
                }
            });

            // 开启手动 ACK
            client.setManualAcks(true);

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(false);
            options.setAutomaticReconnect(config.automaticReconnect);
            options.setConnectionTimeout(config.connectionTimeoutSeconds);
            options.setKeepAliveInterval(config.keepAliveIntervalSeconds);
            if (config.username != null) options.setUserName(config.username);
            if (config.password != null) options.setPassword(config.password);

            log.info("[MqttSubscribeUtil] 开始连接 broker={} clientId={} cleanSession=false", broker, clientId);
            client.connect(options);
            log.info("[MqttSubscribeUtil] 连接成功 clientId={}", clientId);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] 初始化失败 broker={} clientId={} reasonCode={} {}",
                    broker, clientId, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("MqttSubscribeUtil 初始化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 同步订阅一个主题 filter。方法阻塞直到 broker 返回 SUBACK，或超时/失败抛异常。
     *
     * <p>支持普通主题（含通配符）和共享订阅前缀（$queue/、$share/group/）。
     *
     * <p>消息到达后通过 {@code handler.onMessage(topic, message, acker)} 交付。
     */
    public void subscribe(String topicFilter, QosLevel qos, MqttMessageHandler handler) {
        if (topicFilter == null || topicFilter.isEmpty())
            throw new IllegalArgumentException("topicFilter 不能为空");
        if (qos == null) throw new IllegalArgumentException("qos 不能为 null");
        if (handler == null) throw new IllegalArgumentException("handler 不能为 null");

        handlers.put(topicFilter, handler);

        try {
            // per-subscription listener：普通主题由 Paho 内部路由直接交付
            IMqttMessageListener listener = (topic, message) -> {
                try {
                    handler.onMessage(topic, message, MqttSubscribeUtil.this::ack);
                } catch (Exception e) {
                    log.warn("[MqttSubscribeUtil] listener 异常（不 ACK，让 broker 重发）" +
                            " topic={} messageId={} {}", topic, message.getId(), e.getMessage());
                }
            };

            log.info("[MqttSubscribeUtil] 开始订阅 filter={} qos={}", topicFilter, qos);
            client.subscribe(topicFilter, qos.value(), listener);
            log.info("[MqttSubscribeUtil] 订阅成功 filter={}", topicFilter);
        } catch (MqttException e) {
            handlers.remove(topicFilter);
            log.error("[MqttSubscribeUtil] subscribe 异常 filter={} qos={} reasonCode={} {}",
                    topicFilter, qos, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("subscribe 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 手动 ACK 一条消息。
     */
    public void ack(int messageId, int qos) throws MqttException {
        client.messageArrivedComplete(messageId, qos);
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

    /**
     * filter 匹配（支持 $queue/、$share/group/ 前缀）。
     */
    private static boolean matchesFilter(String filter, String actualTopic) {
        String effective = filter;
        if (filter.startsWith("$queue/")) {
            effective = filter.substring("$queue/".length());
        } else if (filter.startsWith("$share/")) {
            int first = filter.indexOf('/');
            int second = filter.indexOf('/', first + 1);
            if (second > 0 && second < filter.length() - 1) {
                effective = filter.substring(second + 1);
            }
        }
        return MqttTopic.isMatched(effective, actualTopic);
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

        public MqttSubscribeUtil build() {
            if (config.clientId == null || config.clientId.isEmpty())
                throw new IllegalArgumentException("clientId 不能为空（消费端必须固定且唯一）");
            return new MqttSubscribeUtil(config);
        }
    }

    /**
     * 消息处理器。业务完成后必须通过 Acker.ack() 手动 ACK。
     */
    public interface MqttMessageHandler {
        void onMessage(String topic, MqttMessage message, Acker acker) throws Exception;
    }

    /**
     * ACK 句柄。
     */
    public interface Acker {
        void ack(int messageId, int qos) throws MqttException;
    }
}

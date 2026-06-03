package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import sunyu.util.mqtt.QosLevel;

import java.nio.charset.StandardCharsets;

/**
 * MQTT Client工具类
 *
 * @author SunYu
 */
public class MqttClientUtil implements AutoCloseable {
    private static final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private MqttClientUtil(Config config) {
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());

        try {
            MqttAsyncClient client = new MqttAsyncClient(config.broker, config.clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(config.cleanSession);
            options.setAutomaticReconnect(config.automaticReconnect);
            options.setConnectionTimeout(config.connectionTimeout);
            options.setKeepAliveInterval(config.keepAliveInterval);
            if (config.username != null) {
                options.setUserName(config.username);
            }
            if (config.password != null) {
                options.setPassword(config.password);
            }
            client.setCallback(config.mqttCallback);
            client.connect(options).waitForCompletion((long) config.connectionTimeout * 1000);
            config.mqttClient = client;
        } catch (MqttException e) {
            log.error("初始化MqttClient失败 {}", e.getMessage());
            throw new RuntimeException(e);
        }

        log.info("[构建 {}] 结束", this.getClass().getSimpleName());
        this.config = config;
    }

    private static class Config {
        private String broker = "tcp://broker.emqx.io:1883";
        private String clientId = "client-id";
        private String username;
        private char[] password;
        private boolean automaticReconnect = true;
        private boolean cleanSession = false;
        private int connectionTimeout = 30;
        private int keepAliveInterval = 60;
        private MqttAsyncClient mqttClient;
        private MqttCallback mqttCallback = new MqttCallback() {
            /**
             * 收到消息
             * @param topic name of the topic on the message was published to
             * @param message the actual message.
             * @throws Exception
             */
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                byte[] payload = message.getPayload();
                String content = (payload == null || payload.length == 0) ? "" : new String(payload, StandardCharsets.UTF_8);
                log.debug("messageArrived {} qos:{} {}", topic, message.getQos(), content);
            }

            /**
             * 断开连接
             * @param cause the reason behind the loss of connection.
             */
            public void connectionLost(Throwable cause) {
                log.warn("connectionLost {}", cause.getMessage());
            }

            /**
             * 消息发送完成
             * @param token the delivery token associated with the message.
             */
            public void deliveryComplete(IMqttDeliveryToken token) {
                log.debug("deliveryComplete {}", token.isComplete());
            }
        };
    }

    public static class Builder {
        private final Config config = new Config();

        public MqttClientUtil build() {
            return new MqttClientUtil(config);
        }

        /**
         * 设置borker地址和端口
         *
         * @param broker
         * @return
         */
        public Builder setBroker(String broker) {
            config.broker = broker;
            return this;
        }

        /**
         * 设置客户端id
         *
         * @param clientId 要求全局唯一
         * @return
         */
        public Builder setClientId(String clientId) {
            config.clientId = clientId;
            return this;
        }

        /**
         * 设置回调方法
         *
         * @param mqttCallback
         * @return
         */
        public Builder setMqttCallback(MqttCallback mqttCallback) {
            config.mqttCallback = mqttCallback;
            return this;
        }

        /**
         * 设置用户名
         *
         * @param username
         * @return
         */
        public Builder setUsername(String username) {
            config.username = username;
            return this;
        }

        /**
         * 设置密码
         *
         * @param password
         * @return
         */
        public Builder setPassword(char[] password) {
            config.password = password;
            return this;
        }

        /**
         * 设置密码（String 形式，内部转为 char[]）
         *
         * @param password
         * @return
         */
        public Builder setPassword(String password) {
            config.password = password == null ? null : password.toCharArray();
            return this;
        }

        /**
         * 是否启用自动重连，默认 true
         *
         * @param automaticReconnect
         * @return
         */
        public Builder setAutomaticReconnect(boolean automaticReconnect) {
            config.automaticReconnect = automaticReconnect;
            return this;
        }

        /**
         * 是否启用 clean session，默认 false（断线重连时保留 in-flight 消息和订阅）
         *
         * @param cleanSession
         * @return
         */
        public Builder setCleanSession(boolean cleanSession) {
            config.cleanSession = cleanSession;
            return this;
        }

        /**
         * 设置连接超时时间（秒），默认 30
         *
         * @param connectionTimeout
         * @return
         */
        public Builder setConnectionTimeout(int connectionTimeout) {
            config.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * 设置心跳间隔（秒），默认 60
         *
         * @param keepAliveInterval
         * @return
         */
        public Builder setKeepAliveInterval(int keepAliveInterval) {
            config.keepAliveInterval = keepAliveInterval;
            return this;
        }
    }

    /**
     * 回收资源。优先"优雅断开"（等待 in-flight 消息处理完再发 DISCONNECT），
     * 优雅断开失败再退化为"强制断开"，最大限度避免 QoS 1/2 消息丢失。
     */
    @Override
    public void close() {
        log.info("[销毁 {}] 开始", this.getClass().getSimpleName());

        try {
            if (config.mqttClient.isConnected()) {
                // quiesceTimeout：Paho 在发 DISCONNECT 之前，给 in-flight 消息留的处理时间（毫秒）
                // totalTimeout：waitForCompletion 的总等待时间（毫秒）
                long quiesceTimeout = Math.max(10_000L, (long) config.keepAliveInterval * 1000);
                long totalTimeout = (long) (config.connectionTimeout + config.keepAliveInterval) * 1000;

                IMqttToken token = config.mqttClient.disconnect(quiesceTimeout);
                token.waitForCompletion(totalTimeout);
                log.debug("disconnect 完成 quiesce={}ms totalTimeout={}ms", quiesceTimeout, totalTimeout);
            }
        } catch (MqttException e) {
            log.error("disconnect失败 reasonCode={} {}", e.getReasonCode(), e.getMessage());
            // 优雅断开失败 → 兜底：强制断开，防止后续 close() 仍卡住
            try {
                if (config.mqttClient.isConnected()) {
                    long quiesceTimeout = Math.max(10_000L, (long) config.keepAliveInterval * 1000);
                    long disconnectTimeout = Math.max(5_000L, (long) config.connectionTimeout * 1000);
                    config.mqttClient.disconnectForcibly(disconnectTimeout, quiesceTimeout);
                }
            } catch (MqttException ex) {
                log.error("强制disconnect失败 {}", ex.getMessage());
            }
        }

        try {
            config.mqttClient.close();
        } catch (MqttException e) {
            log.error("close失败 {}", e.getMessage());
        }

        log.info("[销毁 {}] 结束", this.getClass().getSimpleName());
    }


    /**
     * 发送消息
     *
     * @param topic 主题
     * @param qos   消息质量等级（QosLevel 枚举，避免非法值）
     * @param msg   消息内容
     * @throws MqttException
     */
    public void publish(String topic, QosLevel qos, String msg) throws MqttException {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (qos == null) {
            throw new IllegalArgumentException("qos 不能为 null");
        }
        if (msg == null) {
            throw new IllegalArgumentException("msg 不能为 null");
        }
        MqttMessage message = new MqttMessage(msg.getBytes(StandardCharsets.UTF_8));
        message.setQos(qos.value());
        config.mqttClient.publish(topic, message).waitForCompletion((long) config.connectionTimeout * 1000);
    }

    /**
     * 订阅消息
     *
     * @param topic 主题
     * @param qos   消息质量等级（QosLevel 枚举，避免非法值）
     * @throws MqttException
     */
    public void subscribe(String topic, QosLevel qos) throws MqttException {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (qos == null) {
            throw new IllegalArgumentException("qos 不能为 null");
        }
        config.mqttClient.subscribe(topic, qos.value()).waitForCompletion((long) config.connectionTimeout * 1000);
    }

    /**
     * 异步操作回调：成功 / 失败都会被通知。
     *
     * <p>两个方法都是 default，调用方只需 override 关心的那个。
     */
    public interface MqttActionListener {
        default void onSuccess() {
        }

        default void onFailure(Throwable cause) {
        }
    }

    /**
     * 异步发送消息：方法立即返回，实际发送结果通过回调通知。
     *
     * @param topic    主题
     * @param qos      消息质量等级
     * @param msg      消息内容
     * @param callback 完成回调（可 null，表示不关心结果）
     */
    public void publishAsync(String topic, QosLevel qos, String msg, MqttActionListener callback) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (qos == null) {
            throw new IllegalArgumentException("qos 不能为 null");
        }
        if (msg == null) {
            throw new IllegalArgumentException("msg 不能为 null");
        }
        byte[] payload = msg.getBytes(StandardCharsets.UTF_8);
        MqttMessage message = new MqttMessage(payload);
        message.setQos(qos.value());

        IMqttActionListener listener = (callback == null) ? null : new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken asyncActionToken) {
                callback.onSuccess();
            }

            @Override
            public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                callback.onFailure(exception);
            }
        };

        try {
            config.mqttClient.publish(topic, message, null, listener);
        } catch (MqttException e) {
            log.error("异步publish发起失败 {} {}", topic, e.getMessage());
            if (callback != null) {
                callback.onFailure(e);
            }
        }
    }

    /**
     * 异步订阅：方法立即返回，订阅结果通过回调通知。
     *
     * @param topic    主题
     * @param qos      消息质量等级
     * @param callback 完成回调（可 null，表示不关心结果）
     */
    public void subscribeAsync(String topic, QosLevel qos, MqttActionListener callback) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (qos == null) {
            throw new IllegalArgumentException("qos 不能为 null");
        }

        IMqttActionListener listener = (callback == null) ? null : new IMqttActionListener() {
            @Override
            public void onSuccess(IMqttToken asyncActionToken) {
                callback.onSuccess();
            }

            @Override
            public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                callback.onFailure(exception);
            }
        };

        try {
            config.mqttClient.subscribe(topic, qos.value(), null, listener);
        } catch (MqttException e) {
            log.error("异步subscribe发起失败 {} {}", topic, e.getMessage());
            if (callback != null) {
                callback.onFailure(e);
            }
        }
    }

}
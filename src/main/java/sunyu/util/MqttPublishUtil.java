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
import java.util.UUID;

/**
 * MQTT <b>消息发布端（Producer）</b>工具类 —— 基于 Eclipse Paho 同步客户端 {@link MqttClient} 实现，
 * 专门用于向 broker 发送 MQTT 消息。
 *
 * <p>设计目标：
 * <ul>
 *   <li>语义清晰：与 {@link MqttSubscribeUtil} 分拆，让"发消息"与"收消息"在两套独立的 clientId / 会话下工作，
 *       避免同一 clientId 同时做发布和订阅导致 broker 侧踢下线。</li>
 *   <li>默认值合理：默认 cleanSession=true（即用即走，broker 不保留本端会话）、automaticReconnect=true
 *       （底层网络抖动时自动指数退避重连），心跳 60 秒，连接超时 30 秒。</li>
 *   <li>类型安全：QoS 通过 {@link QosLevel} 枚举传入，避免误传非法 int 值。</li>
 *   <li>资源安全：实现 {@link AutoCloseable}，配合 try-with-resources 自动释放底层 socket 与线程。</li>
 * </ul>
 *
 * <p><b>核心配置项（通过 Builder 链式设置，未设置则使用默认值 / 自动生成）</b>：
 * <ul>
 *   <li>{@code broker}：默认 {@code tcp://broker.emqx.io:1883}，broker 连接地址。</li>
 *   <li>{@code clientId}：未设置时自动生成 {@code pub-<UUID 截断>}，MQTT 3.1.1 规范限制最大 23 字节；
 *       建议在真实项目中显式设置"服务名 + 实例编号"形式的固定值，便于排查。</li>
 *   <li>{@code username / password}：默认 null；broker 开启鉴权时必须设置；密码在日志中脱敏为 {@code *****}。</li>
 *   <li>{@code cleanSession}：默认 true —— 发布端即用即走，broker 不保留本端会话。</li>
 *   <li>{@code automaticReconnect}：默认 true —— 底层网络抖动时以指数退避自动重连。</li>
 *   <li>{@code connectionTimeoutSeconds}：默认 30 秒 —— connect() 的同步阻塞超时。</li>
 *   <li>{@code keepAliveIntervalSeconds}：默认 60 秒 —— PINGREQ 心跳间隔，保持防火墙会话活跃。</li>
 * </ul>
 *
 * <p><b>快速上手</b>：
 * <pre>
 * try (MqttPublishUtil producer = MqttPublishUtil.builder()
 *         .setBroker("tcp://your-broker:1883")
 *         .setClientId("order-service-producer-01")
 *         .setUsername("user")
 *         .setPassword("secret")
 *         .build()) {
 *
 *     producer.publish("order/created", QosLevel.AT_LEAST_ONCE, "order-123");
 *     producer.publish("order/paid",    QosLevel.AT_LEAST_ONCE, "order-456");
 * }
 * </pre>
 *
 * @author SunYu
 */
public class MqttPublishUtil implements AutoCloseable {

    /**
     * 统一日志入口。通过 hutool {@link LogFactory} 获取，支持在实际项目中切换到 slf4j / logback 等实现。
     */
    private static final Log log = LogFactory.get();

    /**
     * MQTT 3.1.1 规范中 clientId 的最大字节数（23 字节）。
     *
     * <p>许多 broker（如 EMQX、Mosquitto、HiveMQ）仍沿用这一限制；超出时会拒绝 CONNECT。
     * 自动生成逻辑与用户传入值校验均以该常量为准。
     */
    private static final int MAX_CLIENT_ID_LENGTH = 23;

    /**
     * Paho 同步客户端实例：承担建立/断开 TCP 连接、发送 PUBLISH、按 QoS 等级完成协议握手等所有底层工作。
     */
    private final MqttClient client;

    /**
     * 本实例实际使用的 clientId：来自用户显式设置或 {@link Builder#build()} 阶段的自动生成。
     * 作为内部字段保存以便 {@link #getClientId()} 与日志打印。
     */
    private final String clientId;

    /**
     * 私有构造方法 —— 仅由 {@link Builder#build()} 调用。
     *
     * <p>执行顺序：
     * <ol>
     *   <li>创建底层 {@link MqttClient}，指定 broker、clientId 与
     *       {@link MemoryPersistence}（进程内缓存 QoS 1/2 的未完成消息）。</li>
     *   <li>组装 {@link MqttConnectOptions}，注入 cleanSession、自动重连、超时、心跳、鉴权等参数。</li>
     *   <li>打印包含全部初始化参数的 INFO 日志（password 以 {@code *****} 脱敏）。</li>
     *   <li>调用 {@link MqttClient#connect(MqttConnectOptions)} 阻塞直到收到 CONNACK 或超时抛异常。</li>
     *   <li>任何 MqttException 发生时记录 error 日志（附带 reasonCode），封装为 {@link RuntimeException} 抛出。</li>
     * </ol>
     *
     * @param broker                    broker 连接地址（如 {@code tcp://host:1883}）
     * @param clientId                  客户端标识（≤23 字节）
     * @param username                  鉴权用户名，可为 null
     * @param password                  鉴权密码，可为 null
     * @param cleanSession              是否开启新会话
     * @param automaticReconnect        是否开启底层自动重连
     * @param connectionTimeoutSeconds  connect 超时秒数
     * @param keepAliveIntervalSeconds  心跳间隔秒数
     */
    private MqttPublishUtil(String broker, String clientId, String username, char[] password,
                            boolean cleanSession, boolean automaticReconnect, int connectionTimeoutSeconds,
                            int keepAliveIntervalSeconds) {
        this.clientId = clientId;

        try {
            // MemoryPersistence：进程内缓存 QoS 1/2 未完成消息；进程重启后丢失。
            // 如需跨进程重连后补发消息，可替换为 MqttDefaultFilePersistence（文件持久化）。
            client = new MqttClient(broker, clientId, new MemoryPersistence());

            // 组装 MqttConnectOptions：所有参数由 Builder 传入
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(cleanSession);
            options.setAutomaticReconnect(automaticReconnect);
            options.setConnectionTimeout(connectionTimeoutSeconds);
            options.setKeepAliveInterval(keepAliveIntervalSeconds);
            if (username != null) options.setUserName(username);
            if (password != null) options.setPassword(password);

            // 打印初始化全部参数（password 脱敏），便于问题定位
            log.info("[MqttPublishUtil] 开始连接 broker={} clientId={} username={} password={} " +
                            "cleanSession={} automaticReconnect={} connectionTimeoutSec={} keepAliveIntervalSec={}",
                    broker, clientId, username, password != null ? "*****" : "null",
                    cleanSession, automaticReconnect, connectionTimeoutSeconds, keepAliveIntervalSeconds);

            // 阻塞直到收到 CONNACK 或超时；失败抛 MqttException
            client.connect(options);
            log.info("[MqttPublishUtil] 连接成功 clientId={}", clientId);
        } catch (MqttException e) {
            // reasonCode 对照表：可查阅 MqttException 源码或 MQTT 3.1.1 CONNACK 文档
            log.error("[MqttPublishUtil] 初始化失败 broker={} clientId={} reasonCode={} {}",
                    broker, clientId, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("MqttPublishUtil 初始化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 同步发送一条字符串消息 —— 适用于发送 JSON、文本指令等字符串场景。
     *
     * <p>内部实现：将 {@code msg} 按 UTF-8 编码为字节数组后，委托给
     * {@link #publish(String, QosLevel, byte[])} 统一处理。
     *
     * <p>阻塞行为说明（依赖于 QoS）：
     * <ul>
     *   <li>QoS 0：消息写入底层 socket 后几乎立即返回；broker 与订阅端是否收到不可靠。</li>
     *   <li>QoS 1：阻塞直到收到 broker 的 PUBACK；超时未收到时底层按 automaticReconnect 策略自动重连并重发。</li>
     *   <li>QoS 2：阻塞直到四次握手完成（PUBLISH → PUBREC → PUBREL → PUBCOMP）；保证协议层"不丢、不重"。</li>
     * </ul>
     *
     * <p>异常处理：任何 {@link MqttException}（网络异常、权限被拒、超时等）都会被记录 error 日志并封装为
     * {@link RuntimeException} 抛出；调用方可以基于异常类型决定是否重试或降级。
     *
     * @param topic 目标主题。非空、非 null。注意主题名区分大小写。
     * @param qos   消息服务质量等级。由 {@link QosLevel} 保证合法取值。
     * @param msg   消息内容。以 UTF-8 编码为字节数组。不允许 null。
     * @throws IllegalArgumentException topic / qos / msg 非法
     * @throws RuntimeException          底层 MqttException（含网络、权限、超时等）的封装
     */
    public void publish(String topic, QosLevel qos, String msg) {
        if (msg == null) throw new IllegalArgumentException("msg 不能为 null");
        // 字符串消息：按 UTF-8 编码后，统一委托给二进制版本处理
        publish(topic, qos, msg.getBytes(StandardCharsets.UTF_8));
    }


    /**
     * 同步发送一条二进制消息 —— 适用于下发 protobuf、压缩包、图片、自定义二进制协议等非字符串场景。
     *
     * <p>典型使用场景：
     * <pre>
     * byte[] bin = MyProto.newBuilder().setOrderId("o-123").build().toByteArray();
     * producer.publish("order/created", QosLevel.AT_LEAST_ONCE, bin);
     * </pre>
     *
     * <p>阻塞行为说明（依赖于 QoS）：
     * <ul>
     *   <li>QoS 0：消息写入底层 socket 后几乎立即返回；broker 与订阅端是否收到不可靠。</li>
     *   <li>QoS 1：阻塞直到收到 broker 的 PUBACK；超时未收到时底层按 automaticReconnect 策略自动重连并重发。</li>
     *   <li>QoS 2：阻塞直到四次握手完成（PUBLISH → PUBREC → PUBREL → PUBCOMP）；保证协议层"不丢、不重"。</li>
     * </ul>
     *
     * <p>异常处理：任何 {@link MqttException}（网络异常、权限被拒、超时等）都会被记录 error 日志并封装为
     * {@link RuntimeException} 抛出。
     *
     * @param topic   目标主题，非空、非 null
     * @param qos     消息服务质量等级，由 {@link QosLevel} 保证合法取值
     * @param payload 二进制消息体；允许 null，内部会以空数组 {@code new byte[0]} 兜底
     * @throws IllegalArgumentException topic / qos 非法
     * @throws RuntimeException          底层 MqttException（含网络、权限、超时等）的封装
     */
    public void publish(String topic, QosLevel qos, byte[] payload) {
        if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("topic 不能为空");
        if (qos == null) throw new IllegalArgumentException("qos 不能为 null");
        // payload 允许 null，此处做一次兜底，避免传入 null 时底层抛出 NPE
        byte[] bytes = payload != null ? payload : new byte[0];

        MqttMessage message = new MqttMessage(bytes);
        message.setQos(qos.value());

        try {
            client.publish(topic, message);
            log.debug("[MqttPublishUtil] publish 完成 topic={} qos={} payloadLen={}",
                    topic, qos, bytes.length);
        } catch (MqttException e) {
            log.error("[MqttPublishUtil] publish 异常 topic={} qos={} reasonCode={} {}",
                    topic, qos, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("publish 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 返回本实例实际使用的 clientId（来自用户显式设置或自动生成）。
     *
     * @return clientId
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * 底层客户端是否处于已连接状态。
     *
     * <p><b>注意</b>：该值基于 Paho 的内部状态，网络断开瞬间可能仍短暂返回 true；
     * 真正可靠的判定应结合 publish 异常、连接断开回调等信号综合判断。
     *
     * @return true 表示当前 Paho 认为已连接
     */
    public boolean isConnected() {
        return client.isConnected();
    }

    /**
     * 优雅关闭发布端客户端。
     *
     * <p>关闭策略：
     * <ol>
     *   <li>若仍连接，温和断开：给未完成的 QoS 1/2 消息最多 10 秒完成握手。</li>
     *   <li>温和断开失败时，强制执行断开：5 秒内仍未完成则直接关闭底层 socket，最长等待 10 秒。</li>
     *   <li>最后调用 {@link MqttClient#close()} 释放底层线程、socket 与持久化资源。</li>
     *   <li>各阶段的异常均记录为 warn 日志，不向外抛出，避免 try-with-resources 时掩盖业务异常。</li>
     * </ol>
     */
    @Override
    public void close() {
        log.info("[MqttPublishUtil] 关闭 clientId={}", clientId);
        try {
            // 温和断开：给未完成的 QoS 1/2 消息最多 10 秒完成握手
            if (client.isConnected()) client.disconnect(10_000);
        } catch (MqttException e) {
            log.warn("[MqttPublishUtil] disconnect 异常 {}", e.getMessage());
            try {
                // 强制断开兜底：5 秒内未完成则直接关闭 socket，最长等待 10 秒
                client.disconnectForcibly(5000, 10_000);
            } catch (MqttException ex) {
                log.warn("[MqttPublishUtil] 强制断开也失败 {}", ex.getMessage());
            }
        }
        try {
            // 释放底层客户端资源（socket、线程、持久化等）
            client.close();
        } catch (MqttException e) {
            log.warn("[MqttPublishUtil] close 异常 {}", e.getMessage());
        }
        log.info("[MqttPublishUtil] 关闭完成 clientId={}", clientId);
    }

    /**
     * 生成 clientId：前缀 + UUID（去连字符），总长度截断到 {@link #MAX_CLIENT_ID_LENGTH}。
     *
     * <p>发布端前缀固定为 {@code pub}，订阅端前缀为 {@code sub}，便于在 broker 日志中区分两类客户端。
     *
     * @param prefix 标识前缀，null 或空时降级为 {@code mqtt}
     * @return 形如 {@code pub-0a1b2c3d4e5f6a7b8c} 的 clientId，长度 ≤ 23
     */
    static String generateClientId(String prefix) {
        String base = (prefix == null ? "mqtt" : prefix) + "-" + UUID.randomUUID().toString().replace("-", "");
        return base.length() > MAX_CLIENT_ID_LENGTH ? base.substring(0, MAX_CLIENT_ID_LENGTH) : base;
    }

    /**
     * 获取 Builder 实例，用于链式构造 {@link MqttPublishUtil}。
     *
     * @return 全新的 {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder：通过链式调用构造 {@link MqttPublishUtil} 实例。
     *
     * <p>典型用法：
     * <pre>
     * MqttPublishUtil producer = MqttPublishUtil.builder()
     *         .setBroker("tcp://broker:1883")
     *         .setClientId("my-producer")
     *         .setUsername("user")
     *         .setPassword("pwd")
     *         .setCleanSession(true)
     *         .build();
     * </pre>
     *
     * @author SunYu
     */
    public static class Builder {

        /** broker 连接地址。默认指向公共测试 broker，生产环境务必替换。 */
        private String broker = "tcp://broker.emqx.io:1883";

        /** 客户端标识。build 时未设置则自动生成 {@code pub-<UUID 截断>}。 */
        private String clientId;

        /** 鉴权用户名。默认 null（不鉴权）。 */
        private String username;

        /** 鉴权密码。默认 null。 */
        private char[] password;

        /** 底层自动重连开关。默认 true。 */
        private boolean automaticReconnect = true;

        /** connect 同步超时秒数。默认 30。 */
        private int connectionTimeoutSeconds = 30;

        /** 心跳间隔秒数。默认 60。 */
        private int keepAliveIntervalSeconds = 60;

        /**
         * cleanSession 标记。发布端默认 true —— 即用即走，broker 不需要为该 clientId 保留会话。
         * 仅当"同一个发布端跨重启需要保证消息不丢"时考虑设为 false。
         */
        private boolean cleanSession = true;

        /**
         * 设置 broker 连接地址。
         *
         * @param broker 如 {@code tcp://host:1883}、{@code ssl://host:8883}
         * @return this 链式调用
         */
        public Builder setBroker(String broker) {
            this.broker = broker;
            return this;
        }

        /**
         * 设置 clientId。
         *
         * <p>建议在真实项目中显式设置"服务名 + 实例编号"形式的固定值（如 {@code order-service-producer-01}），
         * 便于在 broker 日志中排查问题。未设置时 {@link #build()} 会自动生成 {@code pub-<UUID 截断>}。
         *
         * @param clientId 客户端标识（长度 ≤ 23，超出会在 build 时抛异常）
         * @return this 链式调用
         */
        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /**
         * 设置鉴权用户名。broker 开启鉴权时必须设置。
         *
         * @param username 用户名
         * @return this 链式调用
         */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * 设置鉴权密码（char[] 形式，便于敏感数据擦除）。
         *
         * @param password 密码字符数组
         * @return this 链式调用
         */
        public Builder setPassword(char[] password) {
            this.password = password;
            return this;
        }

        /**
         * 设置鉴权密码（String 形式）。内部会转为 char[] 存储。
         *
         * @param password 密码字符串
         * @return this 链式调用
         */
        public Builder setPassword(String password) {
            this.password = password == null ? null : password.toCharArray();
            return this;
        }

        /**
         * 设置是否开启底层自动重连。默认 true。
         *
         * <p>关闭后网络抖动时需要上层自行处理重连与消息补发；建议保持 true。
         *
         * @param automaticReconnect true 表示开启
         * @return this 链式调用
         */
        public Builder setAutomaticReconnect(boolean automaticReconnect) {
            this.automaticReconnect = automaticReconnect;
            return this;
        }

        /**
         * 设置 connect 的同步超时秒数。默认 30 秒。
         *
         * @param seconds 正整数秒
         * @return this 链式调用
         */
        public Builder setConnectionTimeoutSeconds(int seconds) {
            this.connectionTimeoutSeconds = seconds;
            return this;
        }

        /**
         * 设置心跳间隔秒数。默认 60 秒。
         *
         * <p>心跳间隔应小于 broker 配置的会话超期时间（通常为 2x 心跳），否则 broker 可能在
         * 网络抖动后过早清理会话。
         *
         * @param seconds 正整数秒
         * @return this 链式调用
         */
        public Builder setKeepAliveIntervalSeconds(int seconds) {
            this.keepAliveIntervalSeconds = seconds;
            return this;
        }

        /**
         * 设置 cleanSession 标记。发布端默认 true —— 即用即走。
         *
         * <p>改为 false 意味着：
         * <ul>
         *   <li>broker 会记住本 clientId 的会话；</li>
         *   <li>断线期间未完成的 QoS 1/2 消息将在同 clientId 重连后继续推送。</li>
         * </ul>
         * 对"消息不能丢且需跨重启补发"的发布端可考虑设为 false。
         *
         * @param cleanSession true 表示每次连接开启全新会话
         * @return this 链式调用
         */
        public Builder setCleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }

        /**
         * 构造并连接 MQTT 发布客户端，返回可用的 {@link MqttPublishUtil} 实例。
         *
         * <p>执行逻辑：
         * <ol>
         *   <li>若 clientId 未设置 → 自动生成 {@code pub-<UUID 截断>}（≤23 字节）。</li>
         *   <li>若用户传入的 clientId 超出 23 字节 → 抛 {@link IllegalArgumentException}。</li>
         *   <li>创建底层 {@link MqttClient}，组装连接选项，并阻塞调用 connect()。</li>
         *   <li>任何异常均记录 error 日志后封装为 {@link RuntimeException} 抛出。</li>
         * </ol>
         *
         * @return 已连接可用的 {@link MqttPublishUtil} 实例
         * @throws RuntimeException connect 失败或 clientId 非法；内部含原 MqttException 作为 cause
         */
        public MqttPublishUtil build() {
            if (clientId == null || clientId.isEmpty()) {
                clientId = generateClientId("pub");
                log.info("[MqttPublishUtil] 未设置 clientId，已自动生成：{}", clientId);
            } else if (clientId.length() > MAX_CLIENT_ID_LENGTH) {
                throw new IllegalArgumentException("clientId 超出最大长度 " + MAX_CLIENT_ID_LENGTH
                        + " 字节：当前长度 " + clientId.length() + "，值=\"" + clientId + "\"");
            }
            return new MqttPublishUtil(broker, clientId, username, password,
                    cleanSession, automaticReconnect, connectionTimeoutSeconds, keepAliveIntervalSeconds);
        }
    }
}

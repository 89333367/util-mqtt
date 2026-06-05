package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import sunyu.util.mqtt.QosLevel;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * MQTT <b>订阅消费端（Consumer）</b>工具类 —— 基于 Eclipse Paho MQTT 5 同步客户端 {@link MqttClient} 实现，
 * 专门用于从 broker 订阅主题并消费消息。
 *
 * <p>设计目标：
 * <ul>
 *   <li>与发布端隔离：使用独立的 clientId / 会话，避免"同一 clientId 既发又收"导致 broker 侧踢下线。</li>
 *   <li>默认 cleanStart=false：broker 记住本 clientId 的订阅与未处理的 QoS 1/2 消息，断线重连后继续推送。</li>
 *   <li>自动 ACK：消息经用户定义的 {@link IMqttMessageListener}（即 `messageArrived`）处理后由 Paho 自动确认。</li>
 *   <li>单一职责：订阅端只负责"接收消息并交给用户 handler 处理"；
 *       如需下发指令或把失败消息重新发回原主题，请在 handler 中使用独立的
 *       {@link MqttPublishUtil} 实例完成。</li>
 *   <li>类型安全的 QoS：通过 {@link QosLevel} 枚举传入订阅等级。</li>
 *   <li>资源安全：实现 {@link AutoCloseable}，配合 try-with-resources 自动释放。</li>
 * </ul>
 *
 * <p><b>核心配置项（通过 Builder 链式设置）</b>：
 * <ul>
 *   <li>{@code broker}：默认 {@code tcp://broker.emqx.io:1883}，broker 连接地址。</li>
 *   <li>{@code clientId}：必须显式传入固定值（如 {@code order-service-consumer-01}）；
 *       默认 cleanStart=false，broker 会按此 clientId 持久化订阅与未 ACK 消息，
 *       一旦 clientId 变化，broker 无法匹配旧会话，断线重连期间的消息会丢失。</li>
 *   <li>{@code username / password}：默认 null，broker 开启鉴权时必须设置；密码在日志中脱敏为 {@code *****}。</li>
 *   <li>{@code cleanStart}：默认 false —— broker 记住本 clientId 的订阅与未 ACK 消息。</li>
 *   <li>{@code automaticReconnect}：默认 true —— 底层网络抖动时指数退避自动重连。</li>
 *   <li>{@code connectionTimeoutSeconds}：默认 30 秒。</li>
 *   <li>{@code keepAliveIntervalSeconds}：默认 60 秒。</li>
 * </ul>
 *
 * <p><b>消息处理流程</b>：
 * <ol>
 *   <li>调用方实现 {@link IMqttMessageListener}（即 `messageArrived(topic, message)` 或 lambda），在其中做业务处理。</li>
 *   <li>消息到达后 Paho 自动 ACK（QoS 1/2 由协议层保证），再回调用户 handler。</li>
 *   <li>如需下发指令或把失败消息重新发回原主题，请使用独立的 {@link MqttPublishUtil} 实例，
 *       不要占用订阅端连接。</li>
 * </ol>
 *
 * <p><b>快速上手</b>：
 * <pre>
 * // 1) 先构建一个独立的发布端（下发指令用）
 * try (MqttPublishUtil publisher = MqttPublishUtil.builder()
 *         .setBroker("tcp://your-broker:1883")
 *         .setClientId("order-service-publisher-01")
 *         .build();
 *
 *      // 2) 再构建订阅端，在 handler 中复用上面的 publisher
 *      MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
 *              .setBroker("tcp://your-broker:1883")
 *              .setClientId("order-service-consumer-01")
 *              .setMessageHandler((topic, message) -> {
 *                  try {
 *                      String payload = new String(message.getPayload());
 *                      // 业务处理：入库、解析、验证等
 *                      // 处理中如需下发指令给终端，使用独立的 MqttPublishUtil
 *                      publisher.publish("command/did-" + message.getId(),
 *                                        QosLevel.AT_LEAST_ONCE, "do_something");
 *                  } catch (Exception e) {
 *                      // 业务失败：需要把消息重新发回原主题交给其他实例重试，
 *                      // 同样使用独立的 MqttPublishUtil，不要在订阅端连接上发消息
 *                      publisher.publish(topic, QosLevel.AT_LEAST_ONCE, message.getPayload());
 *                  }
 *              })
 *              .build()) {
 *
 *     consumer.subscribe("$share/group1/order/created", QosLevel.AT_LEAST_ONCE);
 *     consumer.subscribe("order/paid",                 QosLevel.AT_LEAST_ONCE);
 *
 *     Thread.currentThread().join(); // 保持主线程存活，实际项目通常由容器维持
 * }
 * </pre>
 *
 * @author SunYu
 */
public class MqttSubscribeUtil implements AutoCloseable {

    /**
     * 统一日志入口。通过 hutool {@link LogFactory} 获取，支持在实际项目中切换到 slf4j / logback 等实现。
     */
    private static final Log log = LogFactory.get();

    /**
     * clientId 的最大字节数（23 字节）。
     *
     * <p>虽然 MQTT 5 协议已不再强制限制 clientId 长度，但许多 broker（如 EMQX / Mosquitto / HiveMQ）
     * 仍沿用此限制；用户显式传入的 clientId 长度校验以此常量为准。
     */
    private static final int MAX_CLIENT_ID_LENGTH = 23;

    /**
     * Paho 同步客户端实例：承担建立/断开 TCP 连接、订阅主题、接收 PUBLISH、
     * 按 QoS 完成协议握手、自动 ACK 等底层工作。
     */
    private final MqttClient client;

    /**
     * 本实例实际使用的 clientId。必须由用户通过 Builder 显式传入，且在多次重启时保持一致。
     */
    private final String clientId;

    /**
     * 私有构造方法 —— 仅由 {@link Builder#build()} 调用。
     *
     * <p>执行顺序：
     * <ol>
     *   <li>创建底层 {@link MqttClient}，指定 broker、clientId 与 {@link MemoryPersistence}。</li>
     *   <li>组装 {@link MqttCallback}：将用户传入的 messageHandler / disconnectedHandler /
     *       mqttErrorOccurredHandler / deliveryCompleteHandler 包装为统一回调；
     *       未传的使用默认行为（日志 / 空实现）。
     *       消息到达时直接调用用户注册的 {@link IMqttMessageListener#messageArrived(String, MqttMessage)}。</li>
     *   <li>组装 {@link MqttConnectionOptions}，注入 cleanStart、自动重连、超时、心跳、鉴权等参数。</li>
     *   <li>打印包含全部初始化参数的 INFO 日志（password 以 {@code *****} 脱敏）。</li>
     *   <li>调用 connect() 阻塞直到收到 CONNACK 或超时抛异常；失败时封装为 {@link RuntimeException}。</li>
     * </ol>
     *
     * <p>消息处理回调说明：
     * <ul>
     *   <li>Paho 已在协议层按 QoS 等级自动完成 ACK（QoS 1 自动回 PUBACK，QoS 2 自动完成
     *       PUBREC → PUBREL → PUBCOMP 流程），因此到达 `messageArrived` 时消息对 broker 而言"已消费"。</li>
     *   <li>如需"失败后交给同组其他实例重试"，请在外部使用独立的 {@link MqttPublishUtil} 把原消息
     *       重新发布到原主题（配合共享订阅 `$share/group/topic` 使用）。</li>
     *   <li>`messageArrived` 由 Paho 内部线程回调，若业务逻辑较重（例如耗时的 DB 操作），
     *       建议将耗时处理提交到自定义线程池，避免阻塞同一 client 的其他消息处理。</li>
     * </ul>
     *
     * @param b 包含全部构建参数的 {@link Builder} 实例
     */
    private MqttSubscribeUtil(Builder b) {
        this.clientId = b.clientId;

        try {
            // MemoryPersistence：进程内缓存未完成握手的 QoS 1/2 消息；进程重启后丢失
            client = new MqttClient(b.broker, clientId, new MemoryPersistence());

            // 组装 MqttCallback：以 messageHandler 为消息处理核心；
            // disconnected / deliveryComplete / mqttErrorOccurred 可通过 Builder 单独覆盖
            final IMqttMessageListener handler = b.messageHandler;
            final Consumer<MqttDisconnectResponse> connLost = b.disconnectedHandler;
            final Consumer<IMqttToken> delivDone = b.deliveryCompleteHandler;
            final Consumer<MqttException> errHandler = b.mqttErrorHandler;

            client.setCallback(new MqttCallback() {
                /**
                 * 正常断开回调。由底层在正常 DISCONNECT 交互完成后触发。
                 */
                @Override
                public void disconnected(MqttDisconnectResponse disconnectResponse) {
                    if (connLost != null) {
                        connLost.accept(disconnectResponse);
                    } else {
                        log.warn("[MqttSubscribeUtil] 连接断开 clientId={} reasonString={}",
                                clientId, disconnectResponse != null ? disconnectResponse.getReasonString() : "null");
                    }
                }

                /**
                 * MQTT 协议层错误回调。当发生协议级错误（如异常报文）时触发；未设置时记录 WARN 日志。
                 */
                @Override
                public void mqttErrorOccurred(MqttException exception) {
                    if (errHandler != null) {
                        errHandler.accept(exception);
                    } else {
                        log.warn("[MqttSubscribeUtil] MQTT 协议层错误 clientId={} {}",
                                clientId, exception != null ? exception.getMessage() : "null");
                    }
                }

                /**
                 * 发送完成回调。订阅端不主动发消息，此回调在本工具类中通常不会触发；
                 * 保留用于扩展或与其他组件集成。用户未传入 deliveryCompleteHandler 时空实现。
                 */
                @Override
                public void deliveryComplete(IMqttToken token) {
                    if (delivDone != null) {
                        delivDone.accept(token);
                    }
                    // 未传则空实现：订阅端默认不关心发送完成
                }

                /**
                 * 连接完成回调。automaticReconnect=true 时，断线重连也会触发该方法；
                 * 参数 reconnect 为 true 代表是重连后回调，false 代表首次 connect 回调。
                 */
                @Override
                public void connectComplete(boolean reconnect, String serverURI) {
                    log.info("[MqttSubscribeUtil] {} 完成 clientId={} serverURI={}",
                            reconnect ? "重连" : "连接", clientId, serverURI);
                }

                /**
                 * 消息到达回调。
                 *
                 * <p>Paho 已在协议层按 QoS 等级自动完成 ACK（QoS 1 自动回 PUBACK /
                 * QoS 2 自动完成 PUBREC → PUBREL → PUBCOMP 流程），因此到达这里时消息对 broker 而言
                 * 已"消费完成"。
                 *
                 * <p>handler 中你可以：
                 * <ul>
                 *   <li>做业务处理（入库、验证等）；</li>
                 *   <li>如需下发指令给终端或把失败消息重新发回原主题，
                 *       请使用外部独立的 {@link MqttPublishUtil}，不要占用订阅端的连接。</li>
                 * </ul>
                 */
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    handler.messageArrived(topic, message);
                }

                /**
                 * 认证令牌回调。默认不启用 Enhanced Authentication，空实现即可。
                 */
                @Override
                public void authPacketArrived(int reasonCode, MqttProperties properties) {
                    // 默认空实现：未启用 Enhanced Authentication 时不会触发
                }
            });

            // 组装 MqttConnectionOptions（MQTT 5 版本）
            MqttConnectionOptions options = new MqttConnectionOptions();
            options.setCleanStart(b.cleanStart);
            options.setAutomaticReconnect(b.automaticReconnect);
            options.setConnectionTimeout(b.connectionTimeoutSeconds);
            options.setKeepAliveInterval(b.keepAliveIntervalSeconds);
            if (b.username != null) options.setUserName(b.username);
            if (b.password != null)
                options.setPassword(new String(b.password).getBytes(StandardCharsets.UTF_8));

            log.info("[MqttSubscribeUtil] 开始连接 broker={} clientId={} username={} password={} " +
                            "cleanStart={} automaticReconnect={} connectionTimeoutSec={} keepAliveIntervalSec={}",
                    b.broker, clientId, b.username, b.password != null ? "*****" : "null",
                    b.cleanStart, b.automaticReconnect, b.connectionTimeoutSeconds, b.keepAliveIntervalSeconds);

            // 阻塞直到收到 CONNACK 或超时；失败抛 MqttException
            client.connect(options);
            log.info("[MqttSubscribeUtil] 连接成功 clientId={}", clientId);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] 初始化失败 broker={} clientId={} reasonCode={} {}",
                    b.broker, clientId, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("MqttSubscribeUtil 初始化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 同步订阅一个主题（或主题过滤器）。方法阻塞直到 broker 返回 SUBACK，或超时/失败抛异常。
     *
     * <p>可重复调用本方法订阅多个主题过滤器；每个主题过滤器的消息都会走同一个
     * {@link IMqttMessageListener}（可在 `messageArrived` 内按主题名做分发）。
     *
     * <p>主题过滤器支持：
     * <ul>
     *   <li>通配符：{@code +} 匹配单层，{@code #} 匹配多层（必须位于末尾）。</li>
     *   <li>共享订阅前缀：{@code $share/group/topic}（推荐），同组仅一个成员收到消息；
     *       或 {@code $queue/topic}，全部订阅者共同竞争。</li>
     * </ul>
     *
     * @param topicFilter 主题过滤器，如 {@code order/created}、{@code $share/group1/order/+} 等
     * @param qos         订阅时的最大 QoS；broker 会按 min(发布端 QoS, 订阅端 QoS) 实际下发
     * @throws IllegalArgumentException topicFilter / qos 为 null 或空
     * @throws RuntimeException         底层 MqttException 的封装
     */
    public void subscribe(String topicFilter, QosLevel qos) {
        if (topicFilter == null || topicFilter.isEmpty()) throw new IllegalArgumentException("topicFilter 不能为空");
        if (qos == null) throw new IllegalArgumentException("qos 不能为 null");

        try {
            log.info("[MqttSubscribeUtil] 开始订阅 filter={} qos={}", topicFilter, qos);
            // Paho 内部发送 SUBSCRIBE 控制报文并阻塞直到收到 SUBACK
            client.subscribe(topicFilter, qos.value());
            log.info("[MqttSubscribeUtil] 订阅成功 filter={}", topicFilter);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] subscribe 异常 filter={} qos={} reasonCode={} {}",
                    topicFilter, qos, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("subscribe 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 优雅关闭订阅端客户端。
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
        log.info("[MqttSubscribeUtil] 关闭 clientId={}", clientId);
        try {
            // 温和断开：给未完成的 QoS 1/2 消息最多 10 秒完成握手
            if (client.isConnected()) client.disconnect(10_000L);
        } catch (MqttException e) {
            log.warn("[MqttSubscribeUtil] disconnect 异常 {}", e.getMessage());
            try {
                // 强制断开兜底：5 秒内未完成则直接关闭 socket，最长等待 10 秒
                client.disconnectForcibly(5000L, 10_000L);
            } catch (MqttException ex) {
                log.warn("[MqttSubscribeUtil] 强制断开也失败 {}", ex.getMessage());
            }
        }
        try {
            // 释放底层资源（socket、线程、持久化等）
            client.close();
        } catch (MqttException e) {
            log.warn("[MqttSubscribeUtil] close 异常 {}", e.getMessage());
        }
        log.info("[MqttSubscribeUtil] 关闭完成 clientId={}", clientId);
    }

    /**
     * 获取 Builder 实例，用于链式构造 {@link MqttSubscribeUtil}。
     *
     * @return 全新的 {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder：通过链式调用构造 {@link MqttSubscribeUtil} 实例。
     *
     * <p>必需项：{@link Builder#setMessageHandler(IMqttMessageListener)}。其余参数均有合理默认值，可按需覆盖。
     *
     * <p>典型用法：
     * <pre>
     * // 注意：订阅端只做"收消息"，发布消息或下发指令请用独立的 MqttPublishUtil
     * try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
     *         .setBroker("tcp://broker:1883")
     *         .setClientId("my-consumer")
     *         .setMessageHandler((topic, message) -> {
     *             // 业务处理
     *         })
     *         .build()) {
     *     consumer.subscribe("$share/group1/order/created", QosLevel.AT_LEAST_ONCE);
     * }
     * </pre>
     *
     * @author SunYu
     */
    public static class Builder {

        /**
         * broker 连接地址。默认指向公共测试 broker，生产环境务必替换。
         */
        String broker = "tcp://broker.emqx.io:1883";

        /**
         * 客户端标识。必须显式传入固定值（例如 {@code order-service-consumer-01}），
         * 并在多次启动时保持一致，否则 cleanStart=false 场景下断线重连期间的消息会丢失。
         */
        String clientId;

        /**
         * 鉴权用户名。默认 null（不鉴权）。
         */
        String username;

        /**
         * 鉴权密码。默认 null。
         */
        char[] password;

        /**
         * 底层自动重连开关。默认 true。
         */
        boolean automaticReconnect = true;

        /**
         * connect 同步超时秒数。默认 30。
         */
        int connectionTimeoutSeconds = 30;

        /**
         * 心跳间隔秒数。默认 60。
         */
        int keepAliveIntervalSeconds = 60;

        /**
         * cleanStart 标记。订阅者默认 false —— broker 记住本 clientId 的订阅与未 ACK 消息，
         * 重连后继续推送。若改为 true，每次连接都是全新会话，断线期间的消息可能丢失（除非 QoS 1/2 且
         * 有保留消息机制弥补）。
         */
        boolean cleanStart = false;

        /**
         * 消息处理器。build 前必须设置（非 null）。
         * 直接使用 Paho 的 {@link IMqttMessageListener}，即 `messageArrived(String topic, MqttMessage message)`。
         */
        IMqttMessageListener messageHandler;

        /**
         * 连接断开回调。可选；未设置时使用默认行为（打印 WARN 日志 + 底层自动重连）。
         */
        Consumer<MqttDisconnectResponse> disconnectedHandler;

        /**
         * 发送完成回调。可选；订阅端不主动发消息，该回调在本工具类中通常不会触发。
         * 如果外部还有独立的 MqttPublishUtil 实例需要跟踪发送完成，
         * 可在外部单独注册其回调。
         */
        Consumer<IMqttToken> deliveryCompleteHandler;

        /**
         * MQTT 协议层错误回调。可选；未设置时使用默认行为（打印 WARN 日志）。
         */
        Consumer<MqttException> mqttErrorHandler;

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
         * 设置 clientId。订阅端默认 cleanStart=false，必须显式传入固定值，
         * 并在多次启动时保持一致，否则 broker 无法匹配旧会话，断线重连期间的消息会丢失。
         *
         * @param clientId 客户端标识（不能为空，且长度 ≤ 23 字节；超出或为空会在 build 时抛异常）
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
         * <p>关闭后网络抖动时需要上层自行处理重连并重新订阅；建议保持 true。
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
         * 设置 cleanStart 标记。订阅者默认 false。
         *
         * <p>false：broker 记住本 clientId 的订阅与未 ACK 消息，重连后继续推送 —— 适合消息不能丢的消费端。
         * <br>
         * true：每次连接都是全新会话，broker 不缓存状态 —— 断线期间消息可能丢失（除非 QoS 1/2 且有保留消息机制弥补）。
         *
         * @param cleanStart true 表示每次连接开启全新会话
         * @return this 链式调用
         */
        public Builder setCleanStart(boolean cleanStart) {
            this.cleanStart = cleanStart;
            return this;
        }

        /**
         * 设置消息处理器（<b>必需项</b>）。只需实现一个方法；
         * 注意：订阅端只负责"收消息"，如需下发指令或把失败消息重新发布，
         * 请在外部构建独立的 {@link MqttPublishUtil} 实例。
         *
         * <p>示例：
         * <pre>
         * builder.setMessageHandler((topic, message) -> {
         *     try {
         *         // 业务处理：入库、解析、验证等
         *         String payload = new String(message.getPayload());
         *         // 如需下发指令或发布失败消息，请使用外部独立的 MqttPublishUtil
         *         // producer.publish("command/did-" + message.getId(), QosLevel.AT_LEAST_ONCE, "do_something");
         *     } catch (Exception e) {
         *         // 如需"失败交给同组其他实例处理"，可使用外部独立的 MqttPublishUtil
         *         // 把原消息重新发回原主题（配合共享订阅 $share/group/topic）
         *         // producer.publish(topic, QosLevel.AT_LEAST_ONCE, message.getPayload());
         *     }
         * });
         * </pre>
         *
         * <p>如果还需要自定义连接断开 / 协议层错误 / 发送完成事件，可配合
         * {@link #setDisconnectedHandler(Consumer)} /
         * {@link #setMqttErrorHandler(Consumer)} /
         * {@link #setDeliveryCompleteHandler(Consumer)} 使用。
         *
         * @param handler 消息处理器；null 将在 {@link #build()} 时抛异常
         * @return this 链式调用
         */
        public Builder setMessageHandler(IMqttMessageListener handler) {
            this.messageHandler = handler;
            return this;
        }

        /**
         * 自定义连接断开回调。不传则使用默认行为（打印 WARN 日志，由 automaticReconnect 底层自动重连）。
         *
         * @param handler 收到连接断开响应（可从中获取 reasonString / returnCode / properties 等信息）
         * @return this 链式调用
         */
        public Builder setDisconnectedHandler(Consumer<MqttDisconnectResponse> handler) {
            this.disconnectedHandler = handler;
            return this;
        }

        /**
         * 自定义 MQTT 协议层错误回调。不传则使用默认行为（打印 WARN 日志）。
         *
         * @param handler 收到协议层异常（含 reasonCode / message 等信息）
         * @return this 链式调用
         */
        public Builder setMqttErrorHandler(Consumer<MqttException> handler) {
            this.mqttErrorHandler = handler;
            return this;
        }

        /**
         * 自定义发送确认回调。订阅端本身不主动发消息，此回调在本工具类中通常不会触发；
         * 保留用于扩展。如果需要跟踪发布端发送完成情况，请在 {@link MqttPublishUtil} 侧设置相关回调。
         *
         * @param handler 收到发送完成的 token（可从中获取 messageId、topic 等信息）
         * @return this 链式调用
         */
        public Builder setDeliveryCompleteHandler(Consumer<IMqttToken> handler) {
            this.deliveryCompleteHandler = handler;
            return this;
        }

        /**
         * 构造并连接 MQTT 订阅客户端，返回可用的 {@link MqttSubscribeUtil} 实例。
         *
         * <p>执行逻辑：
         * <ol>
         *   <li>若 clientId 未设置或为空 → 抛 {@link IllegalArgumentException}（订阅端默认
         *       cleanStart=false，必须使用固定 clientId，否则断线重连时 broker 无法匹配旧会话，
         *       缓存的订阅关系与未推送的消息都会丢失）。</li>
         *   <li>若用户传入的 clientId 超出 23 字节 → 抛 {@link IllegalArgumentException}。</li>
         *   <li>若 messageHandler 未设置 → 抛 {@link IllegalArgumentException}（必须有处理器）。</li>
         *   <li>创建底层 {@link MqttClient}，设置 {@link MqttCallback} 并调用 connect()，
         *       阻塞直到 CONNACK 或超时抛异常。</li>
         * </ol>
         *
         * @return 已连接可用的 {@link MqttSubscribeUtil} 实例
         * @throws RuntimeException connect 失败、clientId 非法或缺少 messageHandler；
         *                          内部含原 MqttException 作为 cause
         */
        public MqttSubscribeUtil build() {
            if (clientId == null || clientId.isEmpty()) {
                throw new IllegalArgumentException("clientId 不能为空：订阅端默认 cleanStart=false，" +
                        "必须显式传入固定 clientId（例如 \"order-service-consumer-01\"），" +
                        "否则断线重连时 broker 无法匹配旧会话，缓存的订阅关系与未推送消息都会丢失。");
            }
            if (clientId.length() > MAX_CLIENT_ID_LENGTH) {
                throw new IllegalArgumentException("clientId 超出最大长度 " + MAX_CLIENT_ID_LENGTH
                        + " 字节：当前长度 " + clientId.length() + "，值=\"" + clientId + "\"");
            }
            if (messageHandler == null)
                throw new IllegalArgumentException("messageHandler 不能为空：必须通过 setMessageHandler(...) 传入");
            return new MqttSubscribeUtil(this);
        }
    }
}

package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import sunyu.util.mqtt.QosLevel;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * MQTT <b>订阅消费端（Consumer）</b>工具类 —— 基于 Eclipse Paho 同步客户端 {@link MqttClient} 实现，
 * 专门用于从 broker 订阅主题并消费消息。
 *
 * <p>设计目标：
 * <ul>
 *   <li>与发布端隔离：使用独立的 clientId / 会话，避免"同一 clientId 既发又收"导致 broker 侧踢下线。</li>
 *   <li>默认 cleanSession=false：broker 记住本 clientId 的订阅与未处理的 QoS 1/2 消息，断线重连后继续推送。</li>
 *   <li>自动 ACK：消息经用户定义的 {@link MessageHandler} 处理后由 Paho 自动确认；业务失败时可调用
 *       {@link #requeue(String, MqttMessage)} 把消息重新发回原主题，配合共享订阅
 *       （{@code $share/group/topic}）实现"失败交给同组其他实例重试"。</li>
 *   <li>类型安全的 QoS：通过 {@link QosLevel} 枚举传入订阅等级。</li>
 *   <li>资源安全：实现 {@link AutoCloseable}，配合 try-with-resources 自动释放。</li>
 * </ul>
 *
 * <p><b>核心配置项（通过 Builder 链式设置）</b>：
 * <ul>
 *   <li>{@code broker}：默认 {@code tcp://broker.emqx.io:1883}，broker 连接地址。</li>
 *   <li>{@code clientId}：未设置时自动生成 {@code sub-<UUID 截断>}；若 cleanSession=false，
 *       建议显式设置"服务名 + 实例编号"形式的固定值，以便 broker 持久化会话。</li>
 *   <li>{@code username / password}：默认 null，broker 开启鉴权时必须设置；密码在日志中脱敏为 {@code *****}。</li>
 *   <li>{@code cleanSession}：默认 false —— broker 记住本 clientId 的订阅与未 ACK 消息。</li>
 *   <li>{@code automaticReconnect}：默认 true —— 底层网络抖动时指数退避自动重连。</li>
 *   <li>{@code connectionTimeoutSeconds}：默认 30 秒。</li>
 *   <li>{@code keepAliveIntervalSeconds}：默认 60 秒。</li>
 * </ul>
 *
 * <p><b>消息处理流程</b>：
 * <ol>
 *   <li>调用方实现 {@link MessageHandler}（或 lambda），在其中做业务处理。</li>
 *   <li>消息到达后 Paho 自动 ACK（QoS 1/2 由协议层保证），再回调用户 handler。</li>
 *   <li>业务处理中如需下发指令给终端，可调用第三个参数 {@code util.publish("command/xxx", QosLevel.AT_LEAST_ONCE, "cmd")}，
 *       在消息处理过程中以同一 client 发送消息。</li>
 *   <li>若业务处理失败，可调用第三个参数 {@code util.requeue(topic, message)}，把消息重新发回原主题，
 *       让 broker 按共享订阅规则重新负载均衡给同组其他订阅者（实现"失败交给别人重试"）。</li>
 * </ol>
 *
 * <p><b>快速上手</b>：
 * <pre>
 * try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
 *         .setBroker("tcp://your-broker:1883")
 *         .setClientId("order-service-consumer-01")
 *         .setMessageHandler((topic, message, util) -> {
 *             try {
 *                 // 业务逻辑：入库、解析、验证等
 *                 String payload = new String(message.getPayload());
 *                 log.info("消费成功 topic={} payload={}", topic, payload);
 *                 // 处理中若需要向终端下发指令，可直接调用 util.publish(...)
 *                 util.publish("command/did-" + message.getId(), QosLevel.AT_LEAST_ONCE, "do_something");
 *             } catch (Exception e) {
 *                 // 业务失败：把消息重新发回原主题，由同组其他订阅者继续处理
 *                 util.requeue(topic, message);
 *             }
 *         })
 *         .build()) {
 *
 *     // 共享订阅示例：$share/group1/order/created 表示加入 group1 组，同组仅一个成员收到消息
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
     * MQTT 3.1.1 规范中 clientId 的最大字节数（23 字节）。
     *
     * <p>许多 broker（如 EMQX / Mosquitto / HiveMQ）仍沿用此限制；自动生成逻辑与用户传入值校验均以此常量为准。
     */
    private static final int MAX_CLIENT_ID_LENGTH = 23;

    /**
     * Paho 同步客户端实例：承担建立/断开 TCP 连接、订阅主题、接收 PUBLISH、
     * 按 QoS 完成协议握手、自动 ACK 等底层工作。
     */
    private final MqttClient client;

    /**
     * 本实例实际使用的 clientId。来自用户显式设置或 {@link Builder#build()} 阶段的自动生成。
     */
    private final String clientId;

    /**
     * 消息处理回调函数式接口。
     *
     * <p>用法：
     * <pre>
     * builder.setMessageHandler((topic, message, util) -> {
     *     // 业务处理：入库、解析、验证等
     *     // 处理中如需下发指令给终端，可直接调用 util.publish("command/xxx", QosLevel.AT_LEAST_ONCE, "cmd")
     *     // 失败时 util.requeue(topic, message); // 把消息重新发回原主题，交给同组其他订阅者继续处理
     * });
     * </pre>
     *
     * <p>参数说明：
     * <ul>
     *   <li>{@code topic}：消息到达时的实际主题名（原样）。</li>
     *   <li>{@code message}：原始 MQTT 消息对象，包含 payload、qos、retained、duplicate、messageId 等元信息。</li>
     *   <li>{@code util}：本订阅工具类实例自身；处理中可调用 {@link #publish(String, QosLevel, String)} 向终端下发指令；
     *       业务失败时可调用 {@link #requeue(String, MqttMessage)} 把消息重新发回原主题，交由同组其他订阅者重试。</li>
     * </ul>
     */
    @FunctionalInterface
    public interface MessageHandler {
        /**
         * 消息到达后由 Paho 在其回调线程中调用此方法。
         *
         * <p>Paho 已在协议层按 QoS 等级自动完成 ACK（QoS 1 自动回 PUBACK，QoS 2 自动完成
         * PUBREC → PUBREL → PUBCOMP 流程），因此到达这里时消息对 broker 而言"已消费"。若业务处理失败需重试，
         * 应在 catch 中显式调用 {@code util.requeue(topic, message)}。
         *
         * <p>注意：本方法由 Paho 内部线程回调，若业务逻辑较重（例如耗时的 DB 操作），建议将
         * 耗时处理提交到自定义线程池，避免阻塞同一 client 的其他消息处理。
         *
         * @param topic   实际主题名
         * @param message 原始 MQTT 消息（含 payload 与 qos 等）
         * @param util    当前订阅者实例，便于在回调中调用 publish / requeue 等工具方法
         * @throws Exception 业务异常；抛出后消息不会自动重复消费（Paho 已自动 ACK），
         *                   如需重试请在 catch 中显式调用 {@code util.requeue(topic, message)}。
         */
        void handle(String topic, MqttMessage message, MqttSubscribeUtil util) throws Exception;
    }

    /**
     * 私有构造方法 —— 仅由 {@link Builder#build()} 调用。
     *
     * <p>执行顺序：
     * <ol>
     *   <li>创建底层 {@link MqttClient}，指定 broker、clientId 与 {@link MemoryPersistence}。</li>
     *   <li>组装 {@link MqttCallback}：将用户传入的 messageHandler / connectionLostHandler /
     *       deliveryCompleteHandler 包装为统一回调；未传的使用默认行为（日志 / 空实现）。</li>
     *   <li>组装 {@link MqttConnectOptions}，注入 cleanSession、自动重连、超时、心跳、鉴权等参数。</li>
     *   <li>打印包含全部初始化参数的 INFO 日志（password 以 {@code *****} 脱敏）。</li>
     *   <li>调用 connect() 阻塞直到收到 CONNACK 或超时抛异常；失败时封装为 {@link RuntimeException}。</li>
     * </ol>
     *
     * @param b 包含全部构建参数的 {@link Builder} 实例
     */
    private MqttSubscribeUtil(Builder b) {
        this.clientId = b.clientId;

        try {
            // MemoryPersistence：进程内缓存未完成握手的 QoS 1/2 消息；进程重启后丢失
            client = new MqttClient(b.broker, clientId, new MemoryPersistence());

            // 组装 MqttCallback：以 messageHandler 为消息处理核心；
            // connectionLost / deliveryComplete 可通过 Builder 单独覆盖
            final MessageHandler handler = b.messageHandler;
            final Consumer<Throwable> connLost = b.connectionLostHandler;
            final Consumer<IMqttDeliveryToken> delivDone = b.deliveryCompleteHandler;

            client.setCallback(new MqttCallback() {
                /**
                 * 连接断开回调。用户传入 connectionLostHandler 则调用用户逻辑；否则打印 WARN 日志，
                 * 由 automaticReconnect=true 的底层机制自动指数退避重连。
                 */
                @Override
                public void connectionLost(Throwable cause) {
                    if (connLost != null) {
                        connLost.accept(cause);
                    } else {
                        log.warn("[MqttSubscribeUtil] 连接断开 clientId={} {}", clientId,
                                cause != null ? cause.getMessage() : "null");
                    }
                }

                /**
                 * 发送完成回调。订阅端通常不主动发消息，
                 * 但通过 {@link MqttSubscribeUtil#publish(String, QosLevel, String)} 或
                 * {@link MqttSubscribeUtil#requeue(String, MqttMessage)} 发出的消息
                 * 完成握手后会走到这里。用户传入 deliveryCompleteHandler 则调用，否则空实现。
                 */
                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    if (delivDone != null) {
                        delivDone.accept(token);
                    }
                    // 未传则空实现：订阅端默认不关心发送完成
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
                 *   <li>处理中如需下发指令给终端设备，可调用 {@code util.publish("command/xxx", QosLevel.AT_LEAST_ONCE, "cmd")}；</li>
                 *   <li>业务失败时可调用 {@code util.requeue(topic, message)} 把消息重新发回原主题，
                 *       让 broker 按共享订阅规则重新负载均衡到同组的其他订阅者。</li>
                 * </ul>
                 */
                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    handler.handle(topic, message, MqttSubscribeUtil.this);
                }
            });

            // 组装 MqttConnectOptions
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
     * {@link MessageHandler}（可在 handler 内按主题名做分发）。
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
     * 把消息原封不动重新发布到原主题 —— 用于"业务失败时交给同组其他订阅者重试"。
     *
     * <p>典型使用场景（配合共享订阅）：
     * <pre>
     * (topic, message, util) -> {
     *     try {
     *         // 业务处理：入库、解析、验证等
     *     } catch (Exception e) {
     *         // 业务失败：把消息重新发回原主题，让同组其他订阅者继续处理
     *         util.requeue(topic, message);
     *     }
     * }
     * </pre>
     *
     * <p>工作原理：本实例作为 MQTT 客户端向原主题发送一条新消息（保留原 payload 和 qos），
     * broker 收到后按共享订阅规则重新负载均衡到同组的某个订阅者（可能就是本实例，也可能是其他实例）。
     * 建议配合共享订阅前缀 {@code $share/group/topic} 使用，避免所有实例都收到一次。
     *
     * <p>与 {@link #publish(String, QosLevel, String)} 的区别：
     * <ul>
     *   <li>{@code requeue}：语义为"把原消息重新入队（重发到原主题）"，复用原消息的 payload 与 qos，
     *       专为失败重试场景设计。</li>
     *   <li>{@code publish}：语义为"下发一条新消息给终端设备"，参数完全自定义，
     *       适合在消息处理中对终端下发指令。</li>
     * </ul>
     *
     * @param topic   消息到达时的实际主题名（原样回传即可）
     * @param message 原消息对象（保留 payload、qos 等元信息）
     * @throws IllegalArgumentException topic / message 为 null，或 topic 为空
     * @throws RuntimeException         底层 MqttException（网络、权限、超时等）的封装
     */
    public void requeue(String topic, MqttMessage message) {
        if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("topic 不能为空");
        if (message == null) throw new IllegalArgumentException("message 不能为 null");
        // 复用原消息的 payload 与 qos；对 broker 而言这是一条独立的新消息
        byte[] payload = message.getPayload() != null ? message.getPayload() : new byte[0];
        int qos = message.getQos();

        try {
            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(qos);
            // 使用本实例自身作为发布者发回原主题；发送完成后会回调 deliveryCompleteHandler（若有）
            client.publish(topic, msg);
            log.info("[MqttSubscribeUtil] requeue：消息已重新发布到原主题 topic={} qos={} payloadLen={}",
                    topic, qos, payload.length);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] requeue 失败 topic={} qos={} reasonCode={} {}",
                    topic, qos, e.getReasonCode(), e.getMessage());
            throw new RuntimeException("requeue 失败: " + e.getMessage(), e);
        }
    }


    /**
     * 同步发送一条字符串消息 —— 用于在消息处理过程中下发指令给终端设备或广播结果。
     *
     * <p>内部实现：将 {@code msg} 按 UTF-8 编码为字节数组后，委托给
     * {@link #publish(String, QosLevel, byte[])} 统一处理。
     *
     * <p>典型使用场景：监听到终端上报状态后，根据业务规则下发控制指令（如 {@code command/did-xxxx}）。
     * <pre>
     * (topic, message, util) -> {
     *     // 处理完毕后下发一条文本指令
     *     util.publish("command/did-" + message.getId(), QosLevel.AT_LEAST_ONCE, "do_something");
     * }
     * </pre>
     *
     * <p>阻塞行为说明（依赖于 QoS）：
     * <ul>
     *   <li>QoS 0：消息写入底层 socket 后几乎立即返回；broker 与订阅端是否收到不可靠。</li>
     *   <li>QoS 1：阻塞直到收到 broker 的 PUBACK；超时未收到时底层按 automaticReconnect 策略自动重连并重发。</li>
     *   <li>QoS 2：阻塞直到四次握手完成（PUBLISH → PUBREC → PUBREL → PUBCOMP），协议层保证"不丢、不重"。</li>
     * </ul>
     *
     * <p><b>注意</b>：本方法在消息处理线程（Paho 的回调线程）中同步调用，因此会阻塞当前消息的处理。
     * 如果下发指令非常耗时或 broker 响应慢，可能影响后续消息处理速度；可考虑将下发逻辑提交到自定义线程池，
     * 或使用 {@link MqttPublishUtil} 单独的发布客户端（避免与订阅共用一个连接）。
     *
     * @param topic 目标主题（通常是终端设备的指令主题，例如 {@code command/did-xxx}），非空、非 null
     * @param qos   消息服务质量等级，由 {@link QosLevel} 保证合法取值
     * @param msg   消息内容（字符串，按 UTF-8 编码为字节数组），不允许 null
     * @throws IllegalArgumentException topic / qos / msg 非法
     * @throws RuntimeException         底层 MqttException（网络、权限、超时等）的封装
     */
    public void publish(String topic, QosLevel qos, String msg) {
        if (msg == null) throw new IllegalArgumentException("msg 不能为 null");
        // 字符串消息：按 UTF-8 编码后，统一委托给二进制版本处理
        publish(topic, qos, msg.getBytes(StandardCharsets.UTF_8));
    }


    /**
     * 同步发送一条二进制消息 —— 适用于下发 protobuf、压缩包、图片、自定义二进制协议等非字符串场景。
     *
     * <p>典型使用场景：监听到终端上报后，根据业务规则下发二进制指令（例如 protobuf 响应、固件分片等）。
     * <pre>
     * byte[] bin = MyProto.newBuilder().setDid("did-123").setCmd("open").build().toByteArray();
     * util.publish("device/did-123/cmd", QosLevel.AT_LEAST_ONCE, bin);
     * </pre>
     *
     * <p>阻塞行为说明（依赖于 QoS）：
     * <ul>
     *   <li>QoS 0：消息写入底层 socket 后几乎立即返回；broker 与订阅端是否收到不可靠。</li>
     *   <li>QoS 1：阻塞直到收到 broker 的 PUBACK；超时未收到时底层按 automaticReconnect 策略自动重连并重发。</li>
     *   <li>QoS 2：阻塞直到四次握手完成（PUBLISH → PUBREC → PUBREL → PUBCOMP），协议层保证"不丢、不重"。</li>
     * </ul>
     *
     * @param topic   目标主题，非空、非 null
     * @param qos     消息服务质量等级，由 {@link QosLevel} 保证合法取值
     * @param payload 二进制消息体；允许 null，内部会以空数组 {@code new byte[0]} 兜底
     * @throws IllegalArgumentException topic / qos 非法
     * @throws RuntimeException         底层 MqttException（网络、权限、超时等）的封装
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
            log.debug("[MqttSubscribeUtil] publish 完成 topic={} qos={} payloadLen={}",
                    topic, qos, bytes.length);
        } catch (MqttException e) {
            log.error("[MqttSubscribeUtil] publish 异常 topic={} qos={} reasonCode={} {}",
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
     * 可靠判定应结合 {@link Builder#setConnectionLostHandler(Consumer)} 回调、订阅异常等综合判断。
     *
     * @return true 表示当前 Paho 认为已连接
     */
    public boolean isConnected() {
        return client.isConnected();
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
            // 温和断开：给未完成的消息最多 10 秒处理完
            if (client.isConnected()) {
                client.disconnect(10_000);
            }
        } catch (MqttException e) {
            log.warn("[MqttSubscribeUtil] disconnect 异常 {}", e.getMessage());
            try {
                // 强制断开兜底：5 秒内未完成则直接关闭 socket，最长等待 10 秒
                client.disconnectForcibly(5000, 10_000);
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
     * 生成 clientId：前缀 + UUID（去连字符），总长度截断到 {@link #MAX_CLIENT_ID_LENGTH}。
     *
     * <p>发布端前缀 {@code pub}，订阅端前缀 {@code sub}，便于在 broker 日志中区分两类客户端。
     *
     * @param prefix 标识前缀，null 或空时降级为 {@code mqtt}
     * @return 形如 {@code sub-0a1b2c3d4e5f6a7b8c} 的 clientId，长度 ≤ 23
     */
    static String generateClientId(String prefix) {
        String base = (prefix == null ? "mqtt" : prefix) + "-" + UUID.randomUUID().toString().replace("-", "");
        return base.length() > MAX_CLIENT_ID_LENGTH ? base.substring(0, MAX_CLIENT_ID_LENGTH) : base;
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
     * <p>必需项：{@link #setMessageHandler(MessageHandler)}。其余参数均有合理默认值，可按需覆盖。
     *
     * <p>典型用法：
     * <pre>
     * try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
     *         .setBroker("tcp://broker:1883")
     *         .setClientId("my-consumer")
     *         .setMessageHandler((topic, message, util) -> {
     *             // 业务逻辑
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
         * 客户端标识。build 时未设置则自动生成 {@code sub-<UUID 截断>}。
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
         * cleanSession 标记。订阅者默认 false —— broker 记住本 clientId 的订阅与未 ACK 消息，
         * 重连后继续推送。若改为 true，每次连接都是全新会话，断线期间的消息可能丢失（除非 QoS 1/2 且
         * 有保留消息机制弥补）。
         */
        boolean cleanSession = false;

        /**
         * 消息处理器。build 前必须设置（非 null）。
         */
        MessageHandler messageHandler;

        /**
         * 连接断开回调。可选；未设置时使用默认行为（打印 WARN 日志 + 底层自动重连）。
         */
        Consumer<Throwable> connectionLostHandler;

        /**
         * 发送完成回调。可选；订阅端通常不需要，仅当需要跟踪 republish 的发送完成时使用。
         */
        Consumer<IMqttDeliveryToken> deliveryCompleteHandler;

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
         * <p>cleanSession=false 时强烈建议显式设置"服务名 + 实例编号"形式的固定值
         * （如 {@code order-service-consumer-01}），以便 broker 按固定 clientId 持久化会话；
         * 未设置时 {@link #build()} 会自动生成 {@code sub-<UUID 截断>}。
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
         * 设置 cleanSession 标记。订阅者默认 false。
         *
         * <p>false：broker 记住本 clientId 的订阅与未 ACK 消息，重连后继续推送 —— 适合消息不能丢的消费端。
         * <br>
         * true：每次连接都是全新会话，broker 不缓存状态 —— 断线期间消息可能丢失（除非 QoS 1/2 且有保留消息机制弥补）。
         *
         * @param cleanSession true 表示每次连接开启全新会话
         * @return this 链式调用
         */
        public Builder setCleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }

        /**
         * 设置消息处理器（<b>必需项</b>）。只需实现一个方法；第三个参数 {@code util} 即本实例，
         * 业务失败时可调用 {@code util.requeue(topic, message)} 重试，或在处理中调用
         * {@code util.publish("command/xxx", QosLevel.AT_LEAST_ONCE, "cmd")} 下发指令。
         *
         * <p>示例：
         * <pre>
         * builder.setMessageHandler((topic, message, util) -> {
         *     try {
         *         // 业务处理：入库、解析、验证等
         *         String payload = new String(message.getPayload());
         *         // 处理中可以下发指令给终端设备
         *         util.publish("command/did-" + message.getId(), QosLevel.AT_LEAST_ONCE, "do_something");
         *     } catch (Exception e) {
         *         // 业务失败：把消息重新发回原主题，交给同组其他订阅者继续处理
         *         util.requeue(topic, message);
         *     }
         * });
         * </pre>
         *
         * <p>如果还需要自定义连接断开 / 发送完成事件，可配合
         * {@link #setConnectionLostHandler(Consumer)} /
         * {@link #setDeliveryCompleteHandler(Consumer)} 使用。
         *
         * @param handler 消息处理器；null 将在 {@link #build()} 时抛异常
         * @return this 链式调用
         */
        public Builder setMessageHandler(MessageHandler handler) {
            this.messageHandler = handler;
            return this;
        }

        /**
         * 自定义连接断开回调。不传则使用默认行为（打印 WARN 日志，由 automaticReconnect 底层自动重连）。
         *
         * @param handler 收到连接断开原因（Throwable）；可在其中发告警、记录指标等
         * @return this 链式调用
         */
        public Builder setConnectionLostHandler(Consumer<Throwable> handler) {
            this.connectionLostHandler = handler;
            return this;
        }

        /**
         * 自定义发送确认回调。订阅端通常不需要；当使用
         * {@link MqttSubscribeUtil#publish(String, QosLevel, String)} 下发指令或
         * {@link MqttSubscribeUtil#requeue(String, MqttMessage)} 把消息重新发出后，
         * 可在这里跟踪发送完成情况。不传则空实现。
         *
         * @param handler 收到发送完成的 token（可从中获取 messageId、topic 等信息）
         * @return this 链式调用
         */
        public Builder setDeliveryCompleteHandler(Consumer<IMqttDeliveryToken> handler) {
            this.deliveryCompleteHandler = handler;
            return this;
        }

        /**
         * 构造并连接 MQTT 订阅客户端，返回可用的 {@link MqttSubscribeUtil} 实例。
         *
         * <p>执行逻辑：
         * <ol>
         *   <li>若 clientId 未设置 → 自动生成 {@code sub-<UUID 截断>}（≤23 字节）。</li>
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

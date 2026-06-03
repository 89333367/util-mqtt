package sunyu.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.eclipse.paho.client.mqttv3.*;
import sunyu.util.mqtt.QosLevel;

import java.nio.charset.StandardCharsets;

/**
 * MQTT 客户端工具类（基于 Eclipse Paho MQTT 3.1.1）。
 *
 * <p>设计目标：
 * <ul>
 *     <li>用 Builder 模式集中配置 broker、凭证、重连、超时等参数，避免散落的魔法值；</li>
 *     <li>用 {@link QosLevel} 枚举替代 int qos，在编译期拦截非法值；</li>
 *     <li>暴露"同步"和"异步"两套 API，调用方按需选择；</li>
 *     <li>实现 {@link AutoCloseable}，可直接在 try-with-resources 中使用；</li>
 *     <li>关闭时走"优雅断开 + quiesce 等待 in-flight 消息"流程，最大限度避免 QoS 1/2 消息丢失。</li>
 * </ul>
 *
 * <p>典型用法：
 * <pre>{@code
 * try (MqttClientUtil client = MqttClientUtil.builder()
 *         .setBroker("tcp://your-broker:1883")
 *         .setClientId("order-service-producer-01")
 *         .setUsername("user")
 *         .setPassword("secret")
 *         .setCleanSession(false)      // 保留 session，断线重连后续传
 *         .setAutomaticReconnect(true)  // 网络抖动时自动重连
 *         .setKeepAliveInterval(60)     // 心跳 60 秒
 *         .setConnectionTimeout(30)
 *         .build()) {
 *
 *     // 同步发送（会阻塞到 broker ACK 或超时）
 *     client.publish("order/created", QosLevel.AT_LEAST_ONCE, "order-123");
 *
 *     // 同步订阅
 *     client.subscribe("order/paid", QosLevel.AT_LEAST_ONCE);
 *
 *     // 异步发送（不阻塞调用线程，结果由回调通知）
 *     client.publishAsync("order/paid", QosLevel.EXACTLY_ONCE, "order-456",
 *             new MqttClientUtil.MqttActionListener() {
 *                 @Override
 *                 public void onSuccess() {
 *                     // 发送成功
 *                 }
 *                 @Override
 *                 public void onFailure(Throwable cause) {
 *                     // 发送失败（含本地发起失败与 broker 回 NACK）
 *                 }
 *             });
 * } // try 结束 -> close() 优雅断开
 * }</pre>
 *
 * <p>线程安全说明：Paho 的 {@link MqttAsyncClient} 对外部同步方法是线程安全的，
 * 但业务应避免在 {@link MqttCallback#messageArrived} 回调线程里
 * 再次执行长时间阻塞操作（如数据库事务）——会阻塞整个 client 的消息循环。
 *
 * <p>注意：本工具类使用 MQTT 3.1.1 协议。broker 侧的 session 保留时长由
 * broker 配置（如 EMQX 的 {@code session_expiry_interval}）决定，
 * 客户端 {@code cleanSession=false} 仅表示"愿意沿用/恢复一个持久 session"。
 *
 * @author SunYu
 */
public class MqttClientUtil implements AutoCloseable {

    private static final Log log = LogFactory.get();

    /**
     * 运行期配置（由 Builder 填充，构造之后不可变）。
     */
    private final Config config;

    /**
     * 获取一个全新的 Builder。
     *
     * <p>推荐使用 try-with-resources 持有 {@code build()} 返回的对象，
     * 以便在退出作用域时自动执行 {@link #close()}。
     *
     * @return 新的 Builder 实例
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 构造（私有，仅由 Builder 调用）。
     *
     * <p>会在当前线程内同步建立连接：创建 {@link MqttAsyncClient}、
     * 设置回调、应用连接选项、阻塞到 {@code CONNACK} 或超时/异常。
     * 任何阶段失败都会抛 {@link RuntimeException}，调用方不会拿到"残废"对象。
     *
     * @param config 构建参数
     */
    private MqttClientUtil(Config config) {
        log.info("[构建 MqttClientUtil] 开始 broker={} clientId={}", config.broker, config.clientId);

        try {
            // 实例化异步客户端（内部处理通信线程/消息循环；同步 API 只是在 token 上等待）
            MqttAsyncClient client = new MqttAsyncClient(config.broker, config.clientId);

            // 组装连接选项：按 Config 字段映射到 Paho 的 MqttConnectOptions
            MqttConnectOptions options = new MqttConnectOptions();
            // 是否使用持久 session：false=保留订阅与 in-flight 消息（断线重连后续传）
            options.setCleanSession(config.cleanSession);
            // 自动重连：断线后由 Paho 按退避策略自动重试，直到 reconnect 上限或连接成功
            options.setAutomaticReconnect(config.automaticReconnect);
            // CONNECT 请求超时（秒）：broker 在该时间内未返回 CONNACK 即视为失败
            options.setConnectionTimeout(config.connectionTimeout);
            // 心跳间隔（秒）：客户端定期发 PINGREQ 维持会话；broker 1.5x 时间未收到即判定离线
            options.setKeepAliveInterval(config.keepAliveInterval);
            // 用户名/密码可选：只有非 null 时才设置，否则沿用 broker 的匿名认证策略
            if (config.username != null) {
                options.setUserName(config.username);
            }
            if (config.password != null) {
                options.setPassword(config.password);
            }

            // 在 connect 之前绑定回调，避免第一次断线/消息到达时回调还未设置
            client.setCallback(config.mqttCallback);

            // connect 返回 IMqttToken，waitForCompletion 阻塞到收到 CONNACK 或超时
            // 注意：此处超时使用 connectionTimeout（秒）*1000 得到毫秒
            client.connect(options).waitForCompletion((long) config.connectionTimeout * 1000);
            config.mqttClient = client;
        } catch (MqttException e) {
            log.error("初始化MqttClient失败 reasonCode={} {}", e.getReasonCode(), e.getMessage());
            // 将受检异常包装为运行时异常，保留完整栈信息便于排障
            throw new RuntimeException("初始化MqttClient失败: " + e.getMessage(), e);
        }

        log.info("[构建 MqttClientUtil] 成功 clientId={}", config.clientId);
        this.config = config;
    }

    /**
     * 运行期配置容器。
     *
     * <p>字段只在 Builder 构建阶段写入，之后对外部工具类只读；
     * 为了可读性，每个字段都附带默认值与语义说明。
     */
    private static class Config {

        /**
         * Broker 地址，形如 {@code tcp://broker.example.com:1883} 或
         * {@code ssl://broker.example.com:8883}。默认使用 EMQX 公共 broker。
         */
        private String broker = "tcp://broker.emqx.io:1883";

        /**
         * 客户端 ID。
         *
         * <p>用于在 broker 侧唯一标识一个持久会话。
         * 当 {@link #cleanSession} 为 false 时，必须保证同一逻辑身份的客户端
         * 始终使用相同的 clientId，否则 broker 会按新 clientId 生成新的 session。
         */
        private String clientId = "client-id";

        /**
         * 用户名（可选）；为 null 表示不启用密码认证。
         */
        private String username;

        /**
         * 密码（可选）；使用 char[] 以便使用后可主动清零，降低堆上明文停留时间。
         */
        private char[] password;

        /**
         * 是否启用自动重连；默认 true。断线后由 Paho 自动按指数退避重试。
         */
        private boolean automaticReconnect = true;

        /**
         * 是否 clean session（默认 false：使用持久 session）。
         *
         * <p>false：断线后重连同一 clientId 可恢复之前的订阅与未确认的 in-flight 消息。
         * true：每次连接都是全新会话，broker 立刻清理该 clientId 的历史状态。
         * 注意：EMQX 仍会按 {@code session_expiry_interval} 清理过期 session，
         * 不会无限期记住。
         */
        private boolean cleanSession = false;

        /**
         * 连接超时时间（秒）。默认 30。
         *
         * <p>控制：① CONNECT → CONNACK 的最大等待时间；② 被 {@link #close()}
         * 用作强制断开的超时参数。业务上应与 broker 的能力/负载相匹配。
         */
        private int connectionTimeout = 30;

        /**
         * 心跳间隔（秒）。默认 60。
         *
         * <p>MQTT 协议建议超时判定为 1.5x keepAliveInterval；
         * 值过小会增加心跳流量，过大则延迟断线检测。
         */
        private int keepAliveInterval = 60;

        /**
         * 底层 Paho 异步客户端实例；由构造函数创建并持有。
         */
        private MqttAsyncClient mqttClient;

        /**
         * 默认 MQTT 事件回调。
         *
         * <p>调用方可以通过 {@link Builder#setMqttCallback(MqttCallback)}
         * 注入自己的实现；未设置时使用下面的无操作实现（只打日志）。
         *
         * <p>线程模型：Paho 在自己的 MQTT 调度线程中调用这些回调，
         * 回调实现应尽量短；耗时操作（IO/数据库/下游服务调用）建议
         * 切换到业务线程池执行，否则会阻塞后续消息的分发与 ACK。
         */
        private MqttCallback mqttCallback = new MqttCallback() {

            /**
             * 服务端推送到达的消息。
             *
             * <p>QoS 0：到达即交付，无 ACK；消息可能丢失不会重放。
             * QoS 1：方法正常返回后 Paho 自动回 PUBACK；抛异常则不会 ACK，
             * broker 会在 session 有效范围内重传。
             * QoS 2：Paho 内部会走完 PUBREC / PUBREL / PUBCOMP 4 次握手。
             *
             * @param topic   消息主题
             * @param message MQTT 消息对象（包含 payload、qos、retained、dup 等）
             * @throws Exception 允许业务抛出异常：Paho 会据此决定是否回 ACK
             */
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                byte[] payload = message.getPayload();
                String content = (payload == null || payload.length == 0)
                        ? "" : new String(payload, StandardCharsets.UTF_8);
                log.debug("messageArrived topic={} qos={} retained={} dup={} payload={}",
                        topic, message.getQos(), message.isRetained(), message.isDuplicate(), content);
            }

            /**
             * 连接丢失回调。
             *
             * <p>Paho 判定底层 TCP 连接中断/keepAlive 超时后触发；
             * 如果启用了 {@code automaticReconnect=true}，Paho 会在这个回调之后
             * 自动开始重连，业务方通常只需在此记录日志、更新健康状态即可。
             *
             * @param cause 断线原因（可能为 null）
             */
            @Override
            public void connectionLost(Throwable cause) {
                log.warn("connectionLost {}", cause == null ? "<unknown>" : cause.getMessage());
            }

            /**
             * 消息发送完成回调。
             *
             * <p>对于 QoS 0：消息发出后立即触发；
             * 对于 QoS 1/2：收到 broker 的 PUBACK / PUBCOMP 后才触发。
             * 调用方可以在这里做"发送成功"的统计/链路追踪收尾。
             *
             * @param token 发送 token：可拿到 messageId、原始消息体、完成状态
             */
            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                log.debug("deliveryComplete messageId={} complete={}",
                        token.getMessageId(), token.isComplete());
            }
        };
    }

    /**
     * {@link MqttClientUtil} 的构建器（Builder）。
     *
     * <p>链式配置；调用方 {@code build()} 成功后得到可用的客户端；
     * 构建失败会抛 {@link RuntimeException}（由构造函数中包装而来）。
     */
    public static class Builder {

        private final Config config = new Config();

        /**
         * 完成构建，返回已连接的 {@link MqttClientUtil}。
         *
         * <p>该方法会在当前线程同步建立 MQTT 连接，
         * 可能阻塞最多 {@code connectionTimeout} 秒。
         *
         * @return 可直接使用的客户端
         * @throws RuntimeException 无法建立连接或凭证无效等情况
         */
        public MqttClientUtil build() {
            return new MqttClientUtil(config);
        }

        /**
         * 设置 broker 地址与端口。
         *
         * <p>支持的协议前缀示例：
         * <ul>
         *     <li>{@code tcp://host:1883}：明文 TCP（MQTT 默认端口）</li>
         *     <li>{@code ssl://host:8883}：TLS 加密（需本地信任证书或配置 SSLSocketFactory）</li>
         * </ul>
         *
         * @param broker broker 地址，如 {@code tcp://broker.emqx.io:1883}
         * @return this
         */
        public Builder setBroker(String broker) {
            config.broker = broker;
            return this;
        }

        /**
         * 设置客户端 ID。
         *
         * <p>该 ID 作为持久 session 的标识。当 {@code cleanSession=false} 时，
         * 同一逻辑角色的客户端必须使用相同的 clientId；不同实例请使用不同 ID，
         * 避免相互踢下线。
         *
         * @param clientId 客户端 ID（建议包含业务含义 + 实例标识）
         * @return this
         */
        public Builder setClientId(String clientId) {
            config.clientId = clientId;
            return this;
        }

        /**
         * 设置自定义事件回调（消息到达 / 连接丢失 / 发送完成）。
         *
         * <p>不设置时使用工具类内置的"只打日志"实现。
         *
         * @param mqttCallback 回调实现；非 null
         * @return this
         */
        public Builder setMqttCallback(MqttCallback mqttCallback) {
            config.mqttCallback = mqttCallback;
            return this;
        }

        /**
         * 设置用户名。设置为 null 表示不启用用户名/密码认证。
         *
         * @param username 用户名（可为 null）
         * @return this
         */
        public Builder setUsername(String username) {
            config.username = username;
            return this;
        }

        /**
         * 设置密码（char[] 形式）。
         *
         * <p>使用 char[] 便于在使用后主动清零，降低在堆上明文驻留时间。
         *
         * @param password 密码（可为 null）
         * @return this
         */
        public Builder setPassword(char[] password) {
            config.password = password;
            return this;
        }

        /**
         * 设置密码（String 形式，内部自动转 char[]）。
         *
         * @param password 密码（可为 null）
         * @return this
         */
        public Builder setPassword(String password) {
            config.password = password == null ? null : password.toCharArray();
            return this;
        }

        /**
         * 是否启用自动重连。
         *
         * <p>默认 true。断线后 Paho 会按指数退避策略自动重连，
         * 并保留原 clientId / 订阅（如果 cleanSession=false）。
         *
         * @param automaticReconnect 是否自动重连
         * @return this
         */
        public Builder setAutomaticReconnect(boolean automaticReconnect) {
            config.automaticReconnect = automaticReconnect;
            return this;
        }

        /**
         * 是否启用 clean session（默认 false）。
         *
         * <p>false：断线后使用同一 clientId 重连可恢复订阅与 in-flight 消息；
         * true：每次连接都是全新会话。
         * 注意：broker 仍会按自身的 {@code session_expiry_interval} 清理 session，
         * 不会无限期记住。
         *
         * @param cleanSession 是否使用干净会话
         * @return this
         */
        public Builder setCleanSession(boolean cleanSession) {
            config.cleanSession = cleanSession;
            return this;
        }

        /**
         * 设置连接超时时间（秒）。默认 30。
         *
         * <p>指发起 CONNECT 后等待 CONNACK 的最大时间；
         * 超出则视为连接失败。
         *
         * @param connectionTimeout 超时（秒），必须为正
         * @return this
         */
        public Builder setConnectionTimeout(int connectionTimeout) {
            config.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * 设置心跳间隔（秒）。默认 60。
         *
         * <p>值过小增加心跳流量与 broker 压力；
         * 过大导致断线判定延迟，消息堆积在 broker 侧。
         *
         * @param keepAliveInterval 心跳间隔（秒），必须为正
         * @return this
         */
        public Builder setKeepAliveInterval(int keepAliveInterval) {
            config.keepAliveInterval = keepAliveInterval;
            return this;
        }
    }

    /**
     * 回收资源：优先优雅断开等待 in-flight 消息，失败再强制断开，最后关闭 client。
     *
     * <p>与 try-with-resources 配合使用最稳妥。不要依赖 GC 或 finalize 来关闭连接。
     *
     * <p>流程：
     * <ol>
     *     <li>若 client 当前已连接，先尝试"优雅断开"：
     *         {@code disconnect(quiesceTimeout)} 会先等在途的 QoS 1/2 消息处理完，
     *         最多等待 {@code quiesceTimeout} 毫秒，然后发送 DISCONNECT；
     *         外层 {@code waitForCompletion(totalTimeout)} 保证整体操作不会无限阻塞。</li>
     *     <li>优雅断开失败（超时 / 网络异常 / broker 无响应）：
     *         回退到 {@code disconnectForcibly}：在限定时间内尽可能处理完 in-flight 消息，
     *         之后无条件关闭底层连接。</li>
     *     <li>无论 1/2 是否成功，最后都 {@code close()} 释放本地资源与线程。</li>
     * </ol>
     */
    @Override
    public void close() {
        log.info("[释放 MqttClientUtil] 开始 clientId={}", config.clientId);

        // 阶段 1：优雅断开（推荐路径）
        try {
            if (config.mqttClient.isConnected()) {
                // quiesce 时长：至少 10 秒，最长按 keepAlive 秒数；给 in-flight 消息留足时间
                long quiesceTimeout = Math.max(10_000L, (long) config.keepAliveInterval * 1000);
                // 整体等待时间：连接超时 + 心跳间隔（毫秒），避免极端情况下长期卡住
                long totalTimeout = (long) (config.connectionTimeout + config.keepAliveInterval) * 1000;

                IMqttToken token = config.mqttClient.disconnect(quiesceTimeout);
                token.waitForCompletion(totalTimeout);
                log.debug("disconnect 完成 quiesce={}ms totalTimeout={}ms", quiesceTimeout, totalTimeout);
            }
        } catch (MqttException e) {
            log.error("disconnect失败 reasonCode={} {}", e.getReasonCode(), e.getMessage());
            // 阶段 2：兜底 —— 优雅断开失败，尝试强制断开
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

        // 阶段 3：释放本地 client 资源（关闭线程、清理持久化等）
        try {
            config.mqttClient.close();
        } catch (MqttException e) {
            log.error("close失败 {}", e.getMessage());
        }

        log.info("[释放 MqttClientUtil] 结束 clientId={}", config.clientId);
    }

    /**
     * 同步发送消息（阻塞到 broker 返回 ACK 或超时）。
     *
     * <p>实现细节：通过 {@link MqttAsyncClient#publish} 发送，并在返回的
     * {@link IMqttToken} 上 {@code waitForCompletion} 等待。超时时间使用
     * {@link Config#connectionTimeout}（秒）× 1000 毫秒。
     *
     * @param topic 发送主题；不能为空或空串
     * @param qos   消息质量等级；不能为 null
     * @param msg   消息内容字符串（按 UTF-8 编码为 payload）；不能为 null
     * @throws MqttException            broker 端异常、参数异常、权限问题等
     * @throws IllegalArgumentException 入参为 null 或 topic 为空
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
        // payload 采用 UTF-8 编码，保证跨语言/平台一致
        MqttMessage message = new MqttMessage(msg.getBytes(StandardCharsets.UTF_8));
        message.setQos(qos.value());
        config.mqttClient.publish(topic, message)
                .waitForCompletion((long) config.connectionTimeout * 1000);
    }

    /**
     * 同步订阅（阻塞到 broker 返回 SUBACK 或超时）。
     *
     * @param topic 订阅主题；支持通配符 {@code +} 与 {@code #}
     * @param qos   订阅时与 broker 协商的最大 QoS（实际交付 QoS 不高于该值与消息发布 QoS 的最小值）
     * @throws MqttException            broker 端异常、权限问题等
     * @throws IllegalArgumentException 入参为 null 或 topic 为空
     */
    public void subscribe(String topic, QosLevel qos) throws MqttException {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("topic 不能为空");
        }
        if (qos == null) {
            throw new IllegalArgumentException("qos 不能为 null");
        }
        config.mqttClient.subscribe(topic, qos.value())
                .waitForCompletion((long) config.connectionTimeout * 1000);
    }

    /**
     * 异步操作回调：用于 {@link #publishAsync} 与 {@link #subscribeAsync} 的结果通知。
     *
     * <p>两个方法都是 {@code default}，调用方只需重写关心的回调即可。
     *
     * <p>线程模型：回调运行在 Paho 自己的 MQTT 调度线程上，不要做长时间阻塞操作；
     * 如需做重/慢操作（写数据库、调用下游 HTTP 服务等），应投递到业务线程池。
     */
    public interface MqttActionListener {

        /**
         * 操作成功回调：消息成功发送或订阅成功后触发。
         */
        default void onSuccess() {
        }

        /**
         * 操作失败回调：无论是本地入队阶段抛出异常，还是 broker 返回 NACK，
         * 都会被统一汇聚到这里。
         *
         * @param cause 失败原因（不会为 null）
         */
        default void onFailure(Throwable cause) {
        }
    }

    /**
     * 异步发送消息：方法立即返回，发送结果通过 {@link MqttActionListener} 通知。
     *
     * <p>注意：
     * <ul>
     *     <li>入参校验失败会直接抛 {@link IllegalArgumentException}（同步失败，不走回调）。</li>
     *     <li>底层提交到 Paho 发送队列时抛 {@link MqttException}：
     *         记录日志并通过 {@code callback.onFailure(e)} 通知调用方。</li>
     *     <li>提交成功但 broker 后续 NACK：通过 Paho 的
     *         {@link IMqttActionListener#onFailure} 路径回调 {@code onFailure}。</li>
     * </ul>
     *
     * @param topic    主题；不能为空或空串
     * @param qos      消息质量等级；不能为 null
     * @param msg      消息内容字符串（按 UTF-8 编码为 payload）；不能为 null
     * @param callback 结果回调；可为 null（表示不关心发送结果，也不会抛异常给调用方）
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

        // 将 Paho 的 IMqttActionListener 适配为我们暴露的简化接口
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
            // userContext 传 null：本工具类暂不需要在 token 上挂自定义上下文
            config.mqttClient.publish(topic, message, null, listener);
        } catch (MqttException e) {
            log.error("异步publish发起失败 topic={} qos={} {}", topic, qos, e.getMessage());
            if (callback != null) {
                callback.onFailure(e);
            }
        }
    }

    /**
     * 异步订阅：方法立即返回，订阅结果通过 {@link MqttActionListener} 通知。
     *
     * @param topic    主题（支持通配符）；不能为空或空串
     * @param qos      消息质量等级；不能为 null
     * @param callback 结果回调；可为 null（表示不关心订阅完成事件）
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
            log.error("异步subscribe发起失败 topic={} qos={} {}", topic, qos, e.getMessage());
            if (callback != null) {
                callback.onFailure(e);
            }
        }
    }
}

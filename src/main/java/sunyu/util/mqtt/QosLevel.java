package sunyu.util.mqtt;

/**
 * MQTT 消息服务质量等级枚举（Quality of Service Level）。
 *
 * <p>本枚举用于在调用 {@code MqttPublishUtil#publish(...)}、
 * {@code MqttSubscribeUtil#subscribe(...)} 等方法时，以类型安全的方式指定 QoS，
 * 避免调用方直接传入裸 int 值从而产生非法值（如 -1、3 等）。
 *
 * <p><b>三个等级语义说明</b>：
 * <ul>
 *   <li><b>{@link #AT_MOST_ONCE}（int=0）</b>：最多一次。消息只发送一次、发送后即丢弃，不做任何确认。消息可能丢失，
 *       但绝不会重复。适用于高频遥测数据、实时行情、不敏感状态更新等可容忍少量丢失的场景。</li>
 *   <li><b>{@link #AT_LEAST_ONCE}（int=1）</b>：至少一次。发送方缓存消息直到收到 broker 的 PUBACK；超时未收到会以
 *       DUP=1 重发。消息不会丢失，但可能重复（尤其网络抖动时）。适用于订单、告警、支付回调等
 *       "不能丢"但可通过幂等去重解决重复问题的业务。</li>
 *   <li><b>{@link #EXACTLY_ONCE}（int=2）</b>：恰好一次。四次握手（PUBLISH → PUBREC → PUBREL → PUBCOMP），
 *       协议层保证"既不丢、也不重复"。代价是更高的延迟、更多的网络往返与内存占用。
 *       适用于计费、金融转账、库存变更等对重复高度敏感的核心业务。
 *       注意：此处的"exactly once"只覆盖 MQTT 协议层；消费端在消息处理完成前崩溃时 broker 仍可能重放消息，
 *       业务层仍需做好幂等保护。</li>
 * </ul>
 *
 * <p><b>发布端 / 订阅端 QoS 的关系</b>：
 * <ul>
 *   <li>发布端的 QoS 决定"发布方 ↔ broker"之间的交付保证；</li>
 *   <li>订阅端的 QoS 决定"broker ↔ 订阅方"之间的交付上限；</li>
 *   <li>最终交付到订阅者的有效 QoS = min(发布端 QoS, 订阅端 QoS)。
 *       例如消息以 QoS 1 发出，订阅端订阅 QoS 0，则 broker 以 QoS 0 转发，可能丢失。</li>
 * </ul>
 *
 * <p><b>与工具类结合的最佳实践</b>：
 * <ul>
 *   <li>发布端：对"不能丢"的消息使用 {@link #AT_LEAST_ONCE}；对高敏感业务使用 {@link #EXACTLY_ONCE}。</li>
 *   <li>订阅端：默认 cleanStart=false 的订阅端只负责"收消息"，不负责发布；业务处理失败时请另外创建
 *       独立的 {@code MqttPublishUtil} 把原消息重新发布到原主题，配合共享订阅
 *       （{@code $share/group/topic}）实现"失败交给同组其他实例重试"。</li>
 * </ul>
 *
 * @author SunYu
 */
public enum QosLevel {

    /**
     * QoS 0 —— 最多一次（At most once）。
     *
     * <p>发送方将消息交给底层 socket 后即丢弃本地状态；broker 收到后直接转发给订阅者，不做任何确认。
     * 消息在网络抖动、broker 重启等情况下可能丢失，但绝不会重复。
     *
     * <p>典型场景：高频遥测传感器读数、实时行情、在线状态广播等可容忍少量丢失的高频数据。
     */
    AT_MOST_ONCE(0),

    /**
     * QoS 1 —— 至少一次（At least once）。
     *
     * <p>发送方缓存消息并等待 broker 的 PUBACK；若在超时时间内未收到 PUBACK，发送方会以 DUP=1 重发。
     * broker 转发给订阅者后同样回 PUBACK。消息不会丢失，但可能重复（尤其网络抖动时）。
     *
     * <p>典型场景：订单创建、告警通知、支付回调等"不能丢"但对重复有一定容忍度（或可做幂等处理）的业务。
     */
    AT_LEAST_ONCE(1),

    /**
     * QoS 2 —— 恰好一次（Exactly once）。
     *
     * <p>四次握手流程：
     * <ol>
     *   <li>发送方 → broker：PUBLISH（包 ID）；broker 本地记录并回 PUBREC。</li>
     *   <li>发送方收到 PUBREC 后释放发送端缓存，发送 PUBREL。</li>
     *   <li>broker 收到 PUBREL 后将消息分发给订阅者，回 PUBCOMP。</li>
     *   <li>发送方收到 PUBCOMP 后释放该包 ID 的记录。</li>
     * </ol>
     * 协议层保证"既不丢、也不重复"，但延迟和资源开销明显高于 QoS 1。
     *
     * <p>典型场景：计费、金融转账、库存变更等对重复高度敏感的核心业务。
     *
     * <p><b>重要提示</b>：此处的"exactly once"仅覆盖 MQTT 协议层。若消费端在消息业务处理完成前崩溃，
     * broker 仍可能重放该消息；业务层还需做好幂等保护。
     */
    EXACTLY_ONCE(2);

    /**
     * 对应的原生 int 值（0 / 1 / 2）。
     *
     * <p>该字段为私有，仅在本类内部及 {@link #value()} 返回中使用；
     * 对外暴露统一通过 {@link #value()} 读取，避免被外部修改。
     */
    private final int value;

    /**
     * 私有构造方法：由 JVM 在首次使用枚举常量时调用。
     *
     * @param value 对应的 int 值
     */
    QosLevel(int value) {
        this.value = value;
    }

    /**
     * 返回对应的 Paho 原生 API int 值（0 / 1 / 2）。
     *
     * <p>本方法供工具类内部调用，也可在调用方直接使用原生 Eclipse Paho API
     * （如 {@code MqttMessage#setQos(int)}、{@code IMqttClient#subscribe(String, int)} 等）
     * 时作为入参。
     *
     * @return 0 / 1 / 2
     */
    public int value() {
        return value;
    }

    /**
     * 根据 int 值反向查找对应的 {@link QosLevel} 枚举常量。
     *
     * <p>主要使用场景：从配置文件、数据库、协议元数据等处读到 int 值后希望转换为类型安全的枚举。
     * 正常业务调用应优先直接使用枚举常量（{@link #AT_MOST_ONCE}、{@link #AT_LEAST_ONCE}、
     * {@link #EXACTLY_ONCE}）。
     *
     * @param qos 0 / 1 / 2
     * @return 对应的 {@link QosLevel}
     * @throws IllegalArgumentException qos 不是 0 / 1 / 2 三者之一
     */
    public static QosLevel of(int qos) {
        for (QosLevel q : QosLevel.values()) {
            if (q.value == qos) {
                return q;
            }
        }
        throw new IllegalArgumentException("非法 QoS 值：必须为 0 / 1 / 2，当前 = " + qos);
    }
}

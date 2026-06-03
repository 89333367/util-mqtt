package sunyu.util.mqtt;

/**
 * MQTT 消息质量等级（Quality of Service，QoS）。
 *
 * <p>Eclipse Paho 原生 API 使用 {@code int} 表示 QoS（0/1/2），
 * 本枚举作为类型安全的入参替代，避免调用方传入非法值（如 3 / -1），
 * 并在语义上让"订阅、发布"两处代码的意图更清晰。
 *
 * <p>三个等级的语义（基于 MQTT 3.1.1 / 5.0 协议规范）：
 * <ul>
 *     <li><b>QoS 0 ({@link #AT_MOST_ONCE})</b>：最多一次。消息仅发送一次，
 *         不做确认。允许丢失，不重复。</li>
 *     <li><b>QoS 1 ({@link #AT_LEAST_ONCE})</b>：至少一次。消息可能被重传，
 *         broker/接收端必须确认（PUBACK），不会丢，但可能重复。</li>
 *     <li><b>QoS 2 ({@link #EXACTLY_ONCE})</b>：最多一次且仅一次。协议层保证
 *         端到端不重复、不丢失（4 次握手 PUBLISH → PUBREC → PUBREL → PUBCOMP）。
 *         注意：此"exactly once"仅覆盖 MQTT 协议层；若业务消费端在处理完消息之前
 *         崩溃重启，仍然可能出现业务层面的重放，需要业务幂等作为最后一道防线。</li>
 * </ul>
 *
 * <p>{@link #value()} 返回的 int 可直接喂给 Paho 的
 * {@code MqttMessage.setQos(int)} / {@code subscribe(String, int)} 等 API。
 *
 * @author SunYu
 */
public enum QosLevel {

    /**
     * QoS 0：最多一次（At most once）。
     *
     * <p>发送方只管发一次，不等待确认；接收方也不会确认。
     * 在网络抖动、broker 重启等场景下，消息可能丢失，不会重复。
     * 适用于可容忍少量丢失的高频遥测数据（如传感器读数）。
     */
    AT_MOST_ONCE(0),

    /**
     * QoS 1：至少一次（At least once）。
     *
     * <p>发送方缓存消息直到收到 PUBACK；如果超时未收到，会重发（重复标志 DUP=1）。
     * 接收方收到后以 PUBACK 确认。消息不会丢失，但可能重复（含重传），
     * 业务消费端需要做幂等处理。适用于订单、告警等"不能丢"的场景。
     */
    AT_LEAST_ONCE(1),

    /**
     * QoS 2：精确一次（Exactly once）。
     *
     * <p>4 次握手：PUBLISH → PUBREC → PUBREL → PUBCOMP。发送方和接收方
     * 各自维护消息状态，确保协议层"既不丢、也不重复"。代价是更高的延迟、
     * 更多的内存与流量。适用于交易、计费等对重复高度敏感的场景。
     *
     * <p>注意：此"exactly once"只覆盖 MQTT 协议层。如果消费端在消息处理完成
     * 前崩溃，broker 仍可能在重连后重放消息；业务消费端仍需要幂等保护。
     */
    EXACTLY_ONCE(2);

    /**
     * 对应 Paho 原生 API 所需的 int 值（0 / 1 / 2）。
     */
    private final int value;

    QosLevel(int value) {
        this.value = value;
    }

    /**
     * 返回对应 Paho 原生 API 的 int 值（0 / 1 / 2）。
     *
     * <p>用于：
     * <ul>
     *     <li>{@code MqttMessage.setQos(int)}</li>
     *     <li>{@code IMqttClient.subscribe(String, int)}</li>
     *     <li>其他需要 int QoS 的 API</li>
     * </ul>
     *
     * @return 0 / 1 / 2
     */
    public int value() {
        return value;
    }

    /**
     * 由 int 值反查 QosLevel 枚举。
     *
     * <p>主要用于"从 broker / 配置 / 消息元数据中读到 int qos，再转回枚举"的场景。
     * 调用方不应直接用它构造入参——应优先直接使用枚举常量。
     *
     * @param qos 0 / 1 / 2
     * @return 对应的 {@link QosLevel}
     * @throws IllegalArgumentException qos 不在 {0, 1, 2} 范围内
     */
    public static QosLevel of(int qos) {
        for (QosLevel q : QosLevel.values()) {
            if (q.value == qos) {
                return q;
            }
        }
        throw new IllegalArgumentException("qos 必须是 0/1/2，当前: " + qos);
    }
}

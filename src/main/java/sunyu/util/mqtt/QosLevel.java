package sunyu.util.mqtt;

/**
 * MQTT 消息质量等级（Quality of Service）
 *
 * <p>为避免调用方传递非法的 qos 值（如 3、-1 等），用该枚举替代 int 入参。
 * 内部仍转换为 int 以适配 Eclipse Paho 原生 API。
 *
 * @author SunYu
 */
public enum QosLevel {
    /**
     * 最多一次（At most once）：消息可能丢失，不会重复
     */
    AT_MOST_ONCE(0),
    /**
     * 至少一次（At least once）：消息不会丢失，可能重复
     */
    AT_LEAST_ONCE(1),
    /**
     * 精确一次（Exactly once）：消息不会丢失，也不会重复
     */
    EXACTLY_ONCE(2);

    private final int value;

    QosLevel(int value) {
        this.value = value;
    }

    /**
     * 获取对应 Paho 原生 int 值
     *
     * @return 0 / 1 / 2
     */
    public int value() {
        return value;
    }

    /**
     * 由 int 值反查枚举，非法值抛 IllegalArgumentException
     *
     * @param qos 0 / 1 / 2
     * @return 对应的 QosLevel
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

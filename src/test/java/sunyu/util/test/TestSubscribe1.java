package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 订阅测试：演示如何订阅消息并在业务成功后手动 ACK。
 *
 * <p>典型流程：
 * <ol>
 *   <li>构造 {@link MqttSubscribeUtil}（cleanSession=false，broker 会记住订阅和未 ACK 的消息）</li>
 *   <li>调用 {@link MqttSubscribeUtil#subscribe(String, QosLevel, MqttSubscribeUtil.MqttMessageHandler)} 同步订阅</li>
 *   <li>消息到达后 handler 被触发，业务成功后调用 {@link MqttSubscribeUtil.Acker#ack(int, int)} 手动 ACK</li>
 *   <li>服务保持运行，持续消费；进程退出或手动调用 close() 断开</li>
 * </ol>
 *
 * @author SunYu
 */
public class TestSubscribe1 {

    static final Log log = LogFactory.get();

    static final String SUB_TOPIC = "$queue/sy/bcld/report";
    static final int EXPECT = 3; // 期望收到的消息条数

    public static void main(String[] args) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(EXPECT);
        final AtomicInteger counter = new AtomicInteger();

        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-001")  // 必须固定且唯一，配合 cleanSession=false
                .build()) {

            // 同步订阅：方法返回即 broker 已 SUBACK
            consumer.subscribe(SUB_TOPIC, QosLevel.AT_LEAST_ONCE,
                    (topic, message, acker) -> {
                        int idx = counter.incrementAndGet();
                        log.info("[收到消息] #{}, topic={}, qos={}, messageId={}, payload={}",
                                idx, topic, message.getQos(), message.getId(), new String(message.getPayload()));
                        // 业务处理（入库、转换、过滤等）
                        // 处理成功 —— 手动 ACK（告诉 broker 这条可以删了）
                        acker.ack(message.getId(), message.getQos());
                        log.info("[ACK 成功] #{}", idx);
                        latch.countDown();
                    });

            log.info("订阅完成，等待消息中... 收到 {} 条后退出，最多等待 120 秒", EXPECT);
            boolean ok = latch.await(120, TimeUnit.SECONDS);
            log.info("结束，是否在时限内完成={}, 实际收到条数={}", ok, counter.get());
        }
    }

    /** JUnit 入口：等价于 main()。先跑本类，再跑 TestPublish 发消息。 */
    @Test
    void t001() throws InterruptedException {
        main(null);
    }
}

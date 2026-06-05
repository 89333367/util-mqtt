package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

/**
 * 失败重试演示：业务处理失败 → 调用 util.republish() 把消息重新发回原主题 →
 * broker 按共享订阅规则重新负载均衡给同组的某个订阅者。
 *
 * @author SunYu
 */
public class TestSubscribe2 {
    static final Log log = LogFactory.get();
    static final String SUB_TOPIC = "$share/group1/" + TestPublish.PUB_TOPIC;

    @Test
    void t001() throws InterruptedException {
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-002")
                // setMessageHandler：第三个参数 util 就是 consumer 自身，直接调 util.republish(...)
                .setMessageHandler((topic, message, util) -> {
                    try {
                        log.info("[收到消息-2] messageId={}, topic={}, payload={}",
                                message.getId(), topic, new String(message.getPayload()));
                        // 模拟业务失败（例如数据库连接异常）
                        throw new RuntimeException("模拟业务失败：入库异常");
                        // 业务成功才会到这里 → 自动 ACK
                    } catch (Exception e) {
                        log.warn("[处理失败-2] {} → 调用 republish() 将消息重新发回原主题",
                                e.getMessage());
                        // 关键：直接用第三个参数 util 调用 republish，不需要 holder
                        util.republish(topic, message);
                    }
                })
                .build()) {

            consumer.subscribe(SUB_TOPIC, QosLevel.AT_LEAST_ONCE);

            log.info("TestSubscribe2 启动完成（业务失败后会 republish → 重新负载均衡）...");
            Thread.currentThread().join();
        }
    }
}

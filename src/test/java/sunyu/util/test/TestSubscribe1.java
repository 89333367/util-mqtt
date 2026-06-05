package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

/**
 * 正常消费演示：订阅消息 → 业务处理 → 自动 ACK。
 *
 * <p>使用推荐的 setMessageHandler 方式：
 * 第三个参数 util 就是 consumer 自身，业务失败时可以直接调 util.republish()。
 *
 * @author SunYu
 */
public class TestSubscribe1 {
    static final Log log = LogFactory.get();
    static final String SUB_TOPIC = "$share/group1/" + TestPublish.PUB_TOPIC;

    @Test
    void t001() throws InterruptedException {
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-001")
                // setMessageHandler：第三个参数就是 consumer 自身，不需要 holder
                .setMessageHandler((topic, message, util) -> {
                    log.info("[收到消息-1] messageId={}, topic={}, payload={}",
                            message.getId(), topic, new String(message.getPayload()));
                    // 业务处理：入库、转换、过滤等
                    // 处理成功 → 自动 ACK；处理失败 catch 里 util.republish(...) 重试
                })
                .build()) {

            consumer.subscribe(SUB_TOPIC, QosLevel.AT_LEAST_ONCE);

            log.info("TestSubscribe1 启动完成，持续消费...");
            Thread.currentThread().join();
        }
    }
}

package sunyu.util.test;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.Test;
import sunyu.util.MqttClientUtil;
import sunyu.util.mqtt.QosLevel;

public class TestClient {

    @Test
    void t001() throws MqttException {
        MqttClientUtil client = MqttClientUtil.builder()
                .setBroker("tcp://your-broker:1883")
                .setClientId("order-service-producer-01")
                .setUsername("user")
                .setPassword("secret")
                .setCleanSession(false)      // 保留 session，断线重连后续传
                .setAutomaticReconnect(true)  // 网络抖动时自动重连
                .setKeepAliveInterval(60)     // 心跳 60 秒
                .setConnectionTimeout(30)
                .build();

        // 同步发送（会阻塞到 broker ACK 或超时）
        client.publish("order/created", QosLevel.AT_LEAST_ONCE, "order-123");

        // 同步订阅
        client.subscribe("order/paid", QosLevel.AT_LEAST_ONCE);

        // 异步发送（不阻塞调用线程，结果由回调通知）
        client.publishAsync("order/paid", QosLevel.EXACTLY_ONCE, "order-456",
                new MqttClientUtil.MqttActionListener() {
                    @Override
                    public void onSuccess() {
                        // 发送成功
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        // 发送失败（含本地发起失败与 broker 回 NACK）
                    }
                });

        client.close();
    }
}

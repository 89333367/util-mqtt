# MQTT 工具类

一个轻量、易用的 MQTT 工具包，基于 **Eclipse Paho** 同步客户端封装，拆分为"发布端"和"订阅端"两个独立工具类，避免同一 clientId 同时做发布与订阅导致 broker 侧踢下线。

## 特性

- `MqttPublishUtil`：消息发布者，默认 `cleanStart=true`
- `MqttSubscribeUtil`：消息订阅者，默认 `cleanStart=false`，断线后 broker 继续缓存本 clientId 的消息
- 类型安全的 QoS：使用 `QosLevel` 枚举，避免误传非法值
- 自动 ACK：消息到达后由 Paho 在 MQTT 5 协议层自动确认
- **订阅端只负责"收消息"**：如需下发指令或"失败时把消息重新发回原主题"，请使用独立的 `MqttPublishUtil` 实例（两个独立 clientId，避免共用连接造成问题）
- **订阅端必须显式传入 clientId**（默认 `cleanStart=false`，broker 会按此 clientId 存储会话，clientId 变化会导致断线期间的消息丢失）；**发布端未设置时自动生成**前缀 `pub-` 的 UUID，并校验用户传入值长度（≤23 字节）
- 实现 `AutoCloseable`，推荐使用 try-with-resources 自动释放底层 socket 与线程
- 初始化日志打印全部参数，密码脱敏为 `*****`

## 环境

- JDK 8 及以上版本

## 依赖

```xml
<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-mqtt</artifactId>
    <!-- {paho.version}_{util.version}_{jdk.version} -->
    <version>1.2.5_1.0_jdk8</version>
    <classifier>shaded</classifier>
</dependency>
```

## 主题（Topic）命名与通配符

- `/`：用于分隔层级，例如 `a/b/c`
- `+`：匹配**单层**任意名称，例如 `a/+/c` 匹配 `a/foo/c`、`a/bar/c`，但不匹配 `a/b/d/c`
- `#`：匹配**多层**任意名称，必须位于过滤器末尾，例如 `a/#` 匹配 `a/`、`a/b`、`a/b/c`

注意：通配符仅用于订阅端的"主题过滤器"，发布端发布时必须使用确切的主题名，不可带通配符。

## 共享订阅

共享订阅允许多个客户端以"竞争消费者"的身份共同订阅同一主题，broker 会把每条消息只发给组内其中一个客户端，从而实现水平扩展。

- `$queue/topic`：最简洁写法，所有订阅 `$queue/topic` 的客户端组成一个默认组，组内竞争消费
- `$share/<group-name>/topic`：显式命名分组。不同组彼此独立，同一组内的客户端竞争消费。推荐写法

> 注意：如果发布的主题以 `/` 开头（例如 `/topic/test`），订阅时需要保留前导斜杠，如 `$share/group1//topic/test`（两个斜杠）。
> 共享订阅的实际消息路由由 broker 完成，客户端只需要按上述前缀格式订阅即可。

---

## 例子

### 例 1：同步发布消息

使用 `MqttPublishUtil` 向 `sy/bcld/report` 主题循环发送 10 条消息，QoS 1。

```java
import sunyu.util.MqttPublishUtil;
import sunyu.util.mqtt.QosLevel;
import cn.hutool.core.thread.ThreadUtil;

public class DemoPublish {
    public static void main(String[] args) {
        // 发布端：cleanSession 默认为 true，即用即走
        MqttPublishUtil producer = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-producer-001")     // 建议显式设置
                .setUsername("user")                   // 可选
                .setPassword("secret")                 // 可选
                .setConnectionTimeoutSeconds(30)
                .setKeepAliveIntervalSeconds(60)
                .build();

        for (int i = 0; i < 10; i++) {
            String payload = "msg-" + i;
            // publish() 是同步阻塞调用：直到 broker 返回 PUBACK 才返回
            producer.publish("sy/bcld/report", QosLevel.AT_LEAST_ONCE, payload);
            System.out.println("[发布成功] payload=" + payload);
            ThreadUtil.sleep(1000);
        }

        producer.close();  // 显式关闭；或使用 try-with-resources 自动关闭
    }
}
```

推荐使用 try-with-resources，无需手动调用 `close()`：

```java
try (MqttPublishUtil producer = MqttPublishUtil.builder()
        .setBroker("tcp://broker.emqx.io:1883")
        .setClientId("demo-producer-001")
        .build()) {
    producer.publish("order/created", QosLevel.AT_LEAST_ONCE, "order-001");
    producer.publish("order/paid",    QosLevel.AT_LEAST_ONCE, "order-002");

    // 若消息体不是字符串（protobuf / 压缩包 / 图片等），使用 byte[] 重载版本：
    byte[] binary = new byte[]{ 0x01, 0x02, 0x03, 0x04 };
    producer.publish("device/did-123/cmd", QosLevel.AT_LEAST_ONCE, binary);
}
```

---

### 例 2：订阅并消费消息（自动 ACK，消息处理中可用独立 `MqttPublishUtil` 下发指令）

使用 `MqttSubscribeUtil` 订阅 `$share/group1/sy/bcld/report`。消息到达后 Paho 会自动 ACK；业务逻辑在 `MessageHandler` 中实现。

**重要**：订阅端工具类只负责"收消息"。处理中如果需要下发指令给终端，或把失败消息重新发回原主题，请另外创建一个独立的 `MqttPublishUtil`（独立 clientId）来发布消息，避免在订阅端连接上做发布。

```java
import sunyu.util.MqttPublishUtil;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class DemoSubscribe {
    public static void main(String[] args) throws InterruptedException {
        // 1) 创建一个独立的发布端（下发指令用）：不同 broker / 不同 clientId
        MqttPublishUtil publisher = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-publisher-001")
                .build();

        // 2) 订阅端：cleanSession 默认为 false，断线后 broker 继续为该 clientId 保留订阅与未 ACK 消息
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-001")
                // setMessageHandler 是必选项，签名：(topic, message) -> void
                .setMessageHandler((topic, message) -> {
                    String payload = new String(message.getPayload());
                    System.out.println("[收到消息] topic=" + topic
                            + " qos=" + message.getQos()
                            + " payload=" + payload);

                    // 1) 业务处理：入库 / 校验 / 解析等
                    //    ...

                    // 2) 处理过程中对终端下发控制指令——使用独立的发布端
                    publisher.publish("command/did-" + message.getId(),
                                      QosLevel.AT_LEAST_ONCE,
                                      "do_something");

                    // 3) 如果消息体不是字符串（protobuf、压缩包、固件分片等），
                    //    使用 byte[] 重载版本（注意：这里调用的是 publisher.publish）
                    byte[] bin = new byte[]{ 0x01, 0x02, 0x03, 0x04 };
                    publisher.publish("device/did-" + message.getId() + "/cmd",
                                      QosLevel.AT_LEAST_ONCE,
                                      bin);
                })
                .build()) {

            // 可以多次调用 subscribe() 订阅多个主题过滤器
            consumer.subscribe("$share/group1/sy/bcld/report", QosLevel.AT_LEAST_ONCE);
            consumer.subscribe("order/paid",                       QosLevel.AT_LEAST_ONCE);

            // 保持主线程存活；在 Spring Boot 等容器中通常不需要此句
            Thread.currentThread().join();
        }
    }
}
```

---

### 例 3：业务失败时使用独立 `MqttPublishUtil` 重新发回原主题（结合共享订阅）

共享订阅下，组内某一实例业务失败（例如数据库异常）时，可以把收到的消息原封不动重新发布到同一主题。broker 会按共享订阅规则把新消息重新负载均衡到同组的其他订阅者，从而实现"失败交给别人重试"。

**关键点**：为避免"在订阅端连接上发送消息"造成的状态混淆，请使用独立的 `MqttPublishUtil`（独立 clientId）来做重新发布。

```java
import sunyu.util.MqttPublishUtil;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

public class DemoSubscribeWithRepublish {
    public static void main(String[] args) throws InterruptedException {
        // 1) 创建一个独立的发布端，专门用于"失败后重新发布回原主题"
        MqttPublishUtil publisher = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-publisher-002")
                .build();

        // 2) 创建订阅端，cleanSession=false
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-002")
                .setMessageHandler((topic, message) -> {
                    try {
                        String payload = new String(message.getPayload());
                        System.out.println("[收到消息] " + payload);

                        // 模拟业务失败
                        if ("bad".equals(payload)) {
                            throw new RuntimeException("模拟业务处理失败");
                        }
                        // 正常处理完毕，Paho 协议层已自动 ACK，无需额外操作
                    } catch (Exception e) {
                        // 关键：业务失败 → 使用独立的 MqttPublishUtil 把原消息重新发回原主题
                        // 注意：此主题必须采用共享订阅（$share/group/xxx）才有意义
                        System.err.println("[处理失败] 触发重新发布：" + e.getMessage());
                        publisher.publish(topic, QosLevel.AT_LEAST_ONCE, message.getPayload());
                    }
                })
                // 可选：监听连接断开事件；默认行为是打印 WARN 日志并由底层自动重连
                .setConnectionLostHandler(cause -> System.err.println("[连接断开] " + cause.getMessage()))
                .build()) {

            consumer.subscribe("$share/group1/sy/bcld/report", QosLevel.AT_LEAST_ONCE);

            Thread.currentThread().join();
        }
    }
}
```

> **典型部署**：启动多个上述实例（不同 clientId，都订阅 `$share/group1/sy/bcld/report`）。某一实例处理失败后，publisher 把消息重新发回原主题，broker 会把新消息随机分配给组内另一个健康实例。

---

### 例 4：同时发布与订阅（两个独立工具类，不同 clientId）

同一进程中，既需要接收消息又需要发送消息时，推荐使用两个独立工具类、两个不同 clientId，避免共用 clientId 导致 broker 状态冲突。

```java
import sunyu.util.MqttPublishUtil;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

public class DemoPubSubTogether {
    public static void main(String[] args) throws InterruptedException {
        // 发布端：独立 clientId，默认 cleanSession=true
        MqttPublishUtil producer = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-producer-100")
                .build();

        // 订阅端：独立 clientId，默认 cleanSession=false
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-100")
                .setMessageHandler((topic, message) -> {
                    String payload = new String(message.getPayload());
                    System.out.println("[收到] topic=" + topic + " payload=" + payload);
                    // 使用独立的发布端 producer 回发响应消息
                    producer.publish("device/response/client-1",
                                     QosLevel.AT_LEAST_ONCE,
                                     "ok");
                })
                .build()) {

            consumer.subscribe("sy/bcld/report", QosLevel.AT_LEAST_ONCE);

            Thread.currentThread().join();
        }
    }
}
```

---

### 例 5：QosLevel 枚举使用说明

| 枚举常量 | 对应 int | 含义 |
| --- | --- | --- |
| `QosLevel.AT_MOST_ONCE` | 0 | 最多一次，允许丢失，不重复 |
| `QosLevel.AT_LEAST_ONCE` | 1 | 至少一次，不丢失但可能重复（推荐绝大多数场景） |
| `QosLevel.EXACTLY_ONCE` | 2 | 恰好一次，协议层保证既不丢也不重；延迟与资源开销更高 |

```java
// 发布端
producer.publish("topic", QosLevel.AT_LEAST_ONCE, "hello");

// 订阅端
consumer.subscribe("topic", QosLevel.AT_LEAST_ONCE);

// 若需要从整数还原枚举（例如读配置文件得到 int qos 后转换）
QosLevel level = QosLevel.of(1);  // 返回 AT_LEAST_ONCE
```

---

### 例 6：IoT 场景 · 监听终端上报 → 业务处理 → 下发控制指令

典型物联网场景：终端设备定时上报自身状态（如温湿度、开关状态等），服务端接收到上报后根据业务规则下发控制指令（如"打开空调"、"保持当前状态"）。

**原则**：`MqttSubscribeUtil` 只负责"接收消息"，所有需要"发消息"的操作（下发指令、失败后重新发布回原主题交给其他实例）统一使用独立的 `MqttPublishUtil`（独立 clientId），不要在订阅端连接上做发布。

```java
import sunyu.util.MqttPublishUtil;
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

/**
 * 假设：
 *   - 终端上报主题： device/did-12345/report（携带 payload 状态 JSON）
 *   - 终端指令主题： device/did-12345/cmd   （服务端下发控制指令到此主题）
 *   - 多实例部署时订阅共享订阅： $share/gateway-device-group/device/+/report
 */
public class DemoIoTSubscribeAndCommand {
    public static void main(String[] args) throws InterruptedException {
        // 1) 先创建一个独立的发布端，专门用于"下发指令 / 失败后重新发布"
        MqttPublishUtil publisher = MqttPublishUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("iot-gateway-pub-001")
                .build();

        // 2) 再创建订阅端，仅负责"收消息"
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("iot-gateway-sub-001")
                .setMessageHandler((topic, message) -> {
                    String payload = new String(message.getPayload());
                    System.out.println("[终端上报] topic=" + topic + " payload=" + payload);

                    try {
                        // 1) 解析并执行业务逻辑（此处仅示例，实际项目可解析 JSON、入库、校验等）
                        String did = extractDidFromTopic(topic);     // 示例：从 topic 中解析 did
                        int temperature = parseTemperature(payload); // 示例：从 payload 中解析温度

                        // 2) 根据业务规则下发控制指令给终端——使用独立的 MqttPublishUtil
                        if (temperature > 30) {
                            publisher.publish("device/" + did + "/cmd",
                                              QosLevel.AT_LEAST_ONCE,
                                              "{\"cmd\":\"turn_on_ac\",\"temp\":26}");
                        } else {
                            publisher.publish("device/" + did + "/cmd",
                                              QosLevel.AT_MOST_ONCE,
                                              "{\"cmd\":\"keep_current_state\"}");
                        }

                    } catch (Exception e) {
                        // 3) 业务失败：使用独立的 MqttPublishUtil 把原消息重新发回原主题，
                        //    交给同组其他订阅者继续处理（注意：仅在使用共享订阅时有效；
                        //    否则由于自身仍在订阅同一 topic，可能形成循环）
                        System.err.println("[处理失败] 触发重新发布：" + e.getMessage());
                        publisher.publish(topic, QosLevel.AT_LEAST_ONCE, message.getPayload());
                    }
                })
                // 可选：监听连接断开事件，例如发送告警
                .setConnectionLostHandler(cause -> System.err.println("[连接断开] " + cause.getMessage()))
                .build()) {

            // 订阅共享订阅：多实例部署时消息只会路由到其中一个实例
            consumer.subscribe("$share/gateway-device-group/device/+/report",
                               QosLevel.AT_LEAST_ONCE);

            System.out.println("[启动] IoT 网关已启动，监听 device/+/report ...");
            Thread.currentThread().join();
        }
    }

    // 以下为演示用的简化实现，实际项目应使用 JSON 库（Jackson / FastJson）等
    private static String extractDidFromTopic(String topic) {
        // topic 形如 device/did-12345/report → 取第 2 段
        String[] parts = topic.split("/");
        return parts.length >= 2 ? parts[1] : "unknown";
    }

    private static int parseTemperature(String payload) {
        // 这里简单假设 payload 形如 "{\"temp\":32}"；实际请使用 JSON 库解析
        int start = payload.indexOf("temp");
        if (start < 0) return -1;
        int colon = payload.indexOf(":", start);
        int end   = payload.indexOf("}", colon);
        return Integer.parseInt(payload.substring(colon + 1, end).trim());
    }
}
```

> **部署建议**：
> 1. 把多实例订阅同一共享订阅 `$share/gateway-device-group/device/+/report`，实现负载均衡。
> 2. "下发指令"统一使用独立的 `MqttPublishUtil`（独立 clientId），避免订阅端连接上做发布带来的状态问题。
> 3. 若对终端指令下发 QPS 很高，建议使用自定义线程池专门处理下发，避免阻塞订阅回调。

---

### Builder 完整参数一览

`MqttPublishUtil.builder()` / `MqttSubscribeUtil.builder()` 均支持以下链式配置：

```java
MqttPublishUtil.builder()
    .setBroker("tcp://host:1883")           // broker 地址，默认 tcp://broker.emqx.io:1883
    .setClientId("my-client")                // 客户端标识；未设置时自动生成 pub-xxx
    .setUsername("user")                     // 鉴权用户名（可选）
    .setPassword("pwd")                      // 鉴权密码（可选，支持 String / char[]）
    .setCleanStart(true)                     // 发布端默认 true，订阅端默认 false（MQTT 5 Clean Start）
    .setAutomaticReconnect(true)             // 默认 true，网络抖动自动指数退避重连
    .setConnectionTimeoutSeconds(30)         // connect 同步超时，默认 30
    .setKeepAliveIntervalSeconds(60)         // 心跳，默认 60，建议小于 broker 的会话超期时间
    .build();

// MqttSubscribeUtil 额外支持
MqttSubscribeUtil.builder()
    .setClientId("my-consumer")              // 客户端标识；必须显式传入（默认 cleanStart=false，不能自动生成）
    .setMessageHandler((topic, message) -> {
        // 正常业务：解析 payload、入库、转发 ...
        // 如需下发指令或把失败消息重新发布，请使用独立的 MqttPublishUtil
    })                                                        // 必需
    .setDisconnectedHandler(resp -> { ... })                 // 可选，连接断开回调（MqttDisconnectResponse）
    .setMqttErrorHandler(ex -> { ... })                      // 可选，MQTT 协议层错误回调（MqttException）
    .setDeliveryCompleteHandler(token -> { ... })             // 可选，订阅端默认不主动发消息，保留用于扩展
    .build();
```

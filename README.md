# MQTT 工具类

一个轻量、易用的 MQTT 工具包，基于 **Eclipse Paho** 同步客户端封装，拆分为"发布端"和"订阅端"两个独立工具类，避免同一 clientId 同时做发布与订阅导致 broker 侧踢下线。

## 特性

- `MqttPublishUtil`：消息发布者，默认 `cleanSession=true`
- `MqttSubscribeUtil`：消息订阅者，默认 `cleanSession=false`，断线后 broker 继续缓存本 clientId 的消息
- 类型安全的 QoS：使用 `QosLevel` 枚举，避免误传非法值
- 自动 ACK：消息到达后由 Paho 在协议层自动确认
- `requeue()`：业务处理失败时将消息原封不动重新发布回原主题，配合共享订阅（`$share/group/topic`）实现"失败交给同组其他实例重试"
- `publish()`：在消息处理过程中可随时调用 `util.publish(...)` 向终端设备下发指令或向其他 topic 广播结果
- clientId 未设置时自动生成（发布端前缀 `pub-`，订阅端前缀 `sub-`），并校验用户传入值长度（≤23 字节）
- 实现 `AutoCloseable`，推荐使用 try-with-resources 自动释放底层 socket 与线程
- 初始化日志打印全部参数，密码脱敏为 `*****`

## 环境

- JDK 8 及以上版本

## 依赖

```xml
<!-- 引入 Eclipse Paho 客户端与 hutool（工具类内部使用 hutool 日志） -->
<dependency>
    <groupId>org.eclipse.paho</groupId>
    <artifactId>org.eclipse.paho.client.mqttv3</artifactId>
    <version>1.2.5</version>
</dependency>

<dependency>
    <groupId>cn.hutool</groupId>
    <artifactId>hutool-all</artifactId>
    <version>5.8.x</version>
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

### 例 2：订阅并消费消息（自动 ACK，可下发指令 / 可 requeue 重试）

使用 `MqttSubscribeUtil` 订阅 `$share/group1/sy/bcld/report`。消息到达后 Paho 会自动 ACK；业务逻辑在 `MessageHandler` 中实现：

- **正常业务**：解析 payload → 入库 / 转发
- **处理中下发指令**：直接调用 `util.publish("command/did-xxx", QosLevel.AT_LEAST_ONCE, "do_something")` 向终端下发控制指令
- **业务失败**：调用 `util.requeue(topic, message)` 把消息重新发回原主题（配合共享订阅，交给同组其他订阅者继续处理）

```java
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class DemoSubscribe {
    public static void main(String[] args) throws InterruptedException {
        // 订阅端：cleanSession 默认为 false，断线后 broker 继续为该 clientId 保留订阅与未 ACK 消息
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-001")
                // setMessageHandler 是必选项；第三个参数 util 即消费者自身
                .setMessageHandler((topic, message, util) -> {
                    String payload = new String(message.getPayload());
                    System.out.println("[收到消息] topic=" + topic
                            + " qos=" + message.getQos()
                            + " payload=" + payload);

                    // 1) 业务处理：入库 / 校验 / 解析等
                    //    ...

                    // 2) 处理过程中可对终端下发控制指令（例如根据上报状态下发动作）
                    //    这里向"command/did-<messageId>"主题下发一条 QoS 1 的指令
                    util.publish("command/did-" + message.getId(),
                                  QosLevel.AT_LEAST_ONCE,
                                  "do_something");

                    // 3) 如果消息体不是字符串（protobuf、压缩包、固件分片等），
                    //    使用 byte[] 重载版本直接发送二进制：
                    byte[] bin = new byte[]{ 0x48, 0x65, 0x6c, 0x6c, 0x6f };
                    util.publish("device/did-" + message.getId() + "/cmd",
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

### 例 3：业务失败时调用 `requeue()` 重试（结合共享订阅）

共享订阅下，组内某一实例业务失败（例如数据库异常）时，可调用 `util.requeue(topic, message)` 将消息原封不动重新发回原主题。broker 会按共享订阅规则把消息重新负载均衡到同组的其他订阅者，实现"失败交给别人重试"。

```java
import sunyu.util.MqttSubscribeUtil;
import sunyu.util.mqtt.QosLevel;

public class DemoSubscribeWithRequeue {
    public static void main(String[] args) throws InterruptedException {
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-002")
                .setMessageHandler((topic, message, util) -> {
                    try {
                        String payload = new String(message.getPayload());
                        System.out.println("[收到消息] " + payload);

                        // 假设这里抛异常模拟业务失败
                        if ("bad".equals(payload)) {
                            throw new RuntimeException("模拟业务处理失败");
                        }
                        // 正常处理完毕，Paho 协议层已自动 ACK，无需额外操作
                    } catch (Exception e) {
                        // 关键：业务失败 → 把消息重新发回原主题，交给同组其他订阅者继续处理
                        //     注意：此主题必须采用共享订阅（$share/group/xxx）才有意义
                        System.err.println("[处理失败] 触发 requeue：" + e.getMessage());
                        util.requeue(topic, message);
                    }
                })
                // 可选：监听连接断开事件；默认行为是打印 WARN 日志并由底层自动重连
                .setConnectionLostHandler(cause -> System.err.println("[连接断开] " + cause.getMessage()))
                // 可选：跟踪"requeue / publish 发出的消息"是否完成 QoS 握手
                .setDeliveryCompleteHandler(token -> {
                    try {
                        System.out.println("[发送完成] messageId=" + token.getMessageId());
                    } catch (Exception ignored) { }
                })
                .build()) {

            consumer.subscribe("$share/group1/sy/bcld/report", QosLevel.AT_LEAST_ONCE);

            Thread.currentThread().join();
        }
    }
}
```

> **典型部署**：启动多个上述实例（不同 clientId，都订阅 `$share/group1/sy/bcld/report`）。某一实例处理失败 `requeue` 后，broker 会把新消息随机分配给组内另一个健康实例。

---

### 例 4：同时发布与订阅（两个独立工具类，不同 clientId）

同一进程中，既需要接收消息又需要发送消息时，推荐使用两个独立工具类、两个不同 clientId，避免共用 clientId 导致 broker 状态冲突。或者直接在 `MqttSubscribeUtil` 的 handler 中调用 `util.publish(...)`（同一个连接同时做订阅和少量下发），下面的示例演示两者组合使用。

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
        MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("demo-consumer-100")
                .setMessageHandler((topic, message, util) -> {
                    String payload = new String(message.getPayload());
                    System.out.println("[收到] topic=" + topic + " payload=" + payload);
                    // 方式 A：使用独立的发布端 producer 回发响应（推荐：与订阅端共用时使用）
                    producer.publish("device/response/client-1", QosLevel.AT_LEAST_ONCE, "ok");
                    // 方式 B：也可以直接使用当前 consumer 自身下发指令（适合"监听 → 下发"同一连接）
                    util.publish("command/did-" + message.getId(), QosLevel.AT_LEAST_ONCE, "do_something");
                })
                .build();

        consumer.subscribe("sy/bcld/report", QosLevel.AT_LEAST_ONCE);

        Thread.currentThread().join();
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

典型物联网场景：终端设备定时上报自身状态（如温湿度、开关状态等），服务端接收到上报后根据业务规则下发控制指令（如"打开制冷"、"上报失败请重试"）。

`MqttSubscribeUtil` 订阅端工具类内部封装了同一个底层 `MqttClient`，因此在 `MessageHandler` 中可直接：

- `util.publish(topic, qos, msg)`：向终端下发全新指令
- `util.requeue(topic, message)`：业务失败时把原消息重新发回原主题（配合共享订阅交给其他实例）

```java
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
        try (MqttSubscribeUtil consumer = MqttSubscribeUtil.builder()
                .setBroker("tcp://broker.emqx.io:1883")
                .setClientId("iot-gateway-001")
                .setMessageHandler((topic, message, util) -> {
                    String payload = new String(message.getPayload());
                    System.out.println("[终端上报] topic=" + topic + " payload=" + payload);

                    try {
                        // 1) 解析并执行业务逻辑（此处仅示例，实际项目可解析 JSON、入库、校验等）
                        String did = extractDidFromTopic(topic);     // 示例：从 topic 中解析 did
                        int temperature = parseTemperature(payload); // 示例：从 payload 中解析温度

                        // 2) 根据业务规则下发控制指令给终端
                        //    这里演示：温度超过 30℃ 下发"打开空调"指令；否则下发"保持当前状态"
                        if (temperature > 30) {
                            util.publish("device/" + did + "/cmd",
                                         QosLevel.AT_LEAST_ONCE,
                                         "{\"cmd\":\"turn_on_ac\",\"temp\":26}");
                        } else {
                            util.publish("device/" + did + "/cmd",
                                         QosLevel.AT_MOST_ONCE,
                                         "{\"cmd\":\"keep_current_state\"}");
                        }

                    } catch (Exception e) {
                        // 3) 业务失败：把原消息重新发回原主题，交给同组其他订阅者继续处理
                        //    注意：只有在使用共享订阅（$share/group/topic）时，消息才会派发给其他实例；
                        //         否则由于自身仍在订阅同一 topic，可能造成自己再收一次 → 形成循环
                        System.err.println("[处理失败] 触发 requeue：" + e.getMessage());
                        util.requeue(topic, message);
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
> 2. 单个实例自身也可能使用 `util.publish(...)` 下发指令；建议 QoS 1（至少一次）作为默认。
> 3. 若对终端指令下发 QPS 很高，建议使用独立的 `MqttPublishUtil` 或自定义线程池专门处理下发，避免阻塞订阅回调。

---

### Builder 完整参数一览

`MqttPublishUtil.builder()` / `MqttSubscribeUtil.builder()` 均支持以下链式配置：

```java
MqttPublishUtil.builder()
    .setBroker("tcp://host:1883")           // broker 地址，默认 tcp://broker.emqx.io:1883
    .setClientId("my-client")                // 客户端标识；未设置时自动生成 pub-xxx / sub-xxx
    .setUsername("user")                     // 鉴权用户名（可选）
    .setPassword("pwd")                      // 鉴权密码（可选，支持 String / char[]）
    .setCleanSession(true)                   // 发布端默认 true，订阅端默认 false
    .setAutomaticReconnect(true)             // 默认 true，网络抖动自动指数退避重连
    .setConnectionTimeoutSeconds(30)         // connect 同步超时，默认 30
    .setKeepAliveIntervalSeconds(60)         // 心跳，默认 60，建议小于 broker 的会话超期时间
    .build();

// MqttSubscribeUtil 额外支持
MqttSubscribeUtil.builder()
    .setMessageHandler((topic, msg, util) -> {
        // 正常业务：解析 payload、入库、转发 ...
        // 处理中可下发指令：util.publish("command/did-xxx", QosLevel.AT_LEAST_ONCE, "cmd")
        // 业务失败可 requeue：util.requeue(topic, msg)
    })                                                        // 必需
    .setConnectionLostHandler(cause -> { ... })               // 可选，连接断开回调
    .setDeliveryCompleteHandler(token -> { ... })             // 可选，publish / requeue 发送完成回调
    .build();
```

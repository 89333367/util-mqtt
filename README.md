# MQTT工具类

## 描述

* MQTT Client 通用工具类

## 环境

* jdk8 x64 及以上版本

## 依赖

```xml

<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-excel</artifactId>
    <!-- {paho.version}_{util.version}_{jdk.version} -->
    <version>1.2.5_1.0_jdk8</version>
    <classifier>shaded</classifier>
</dependency>
```

## 例子

```java
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

// 异步订阅（不阻塞调用线程；订阅成功后 broker 才会按该订阅推送消息）
client.subscribeAsync("order/notify", QosLevel.AT_LEAST_ONCE,
    new MqttClientUtil.MqttActionListener() {
        @Override
        public void onSuccess() {
          // 订阅成功：之后到达的 "order/notify" 消息会通过 mqttCallback.messageArrived 交付
        }
        @Override
        public void onFailure(Throwable cause) {
          // 订阅失败（broker 拒绝、权限不足、超时等）
        }
});

client.close();
```


## topic
```
/：用来表示层次，比如 a/b，a/b/c。
#：表示匹配 >=0 个层次，比如 a/# 就匹配 a/，a/b，a/b/c。单独的一个 # 表示匹配所有。不允许 a# 和 a/#/c。
+：表示匹配一个层次，例如 a/+ 匹配 a/b，a/c，不匹配 a/b/c。单独的一个 + 是允许的，a+ 不允许，也可以和多层通配符一起使用，+/tennis/# 、sport/+/player1 都有有效的。
```


## 共享订阅
```
共享订阅：订阅前缀 $queue/，多个客户端订阅了 $queue/topic，发布者发布到 topic，则只有一个客户端会接收到消息

分组订阅：订阅前缀 $share/<group>/，组客户端订阅了 $share/group1/topic、$share/group2/topic..，发布者发布到 topic，则消息会发布到每个 group 中，但是每个 group 中只有一个客户端会接收到消息

注意： 如果发布的 topic 以 / 开头，例如：/topic/test，需要订阅 $share/group1//topic/test，另外 mica-mqtt 默认随机消息路由，共享订阅的多个客户端会随机收到消息
```
package sunyu.util.test;

import org.junit.jupiter.api.Test;
import sunyu.util.MqttClientUtil;

public class TestClient {

    @Test
    void t001() {
        MqttClientUtil client = MqttClientUtil.builder().build();

        client.close();
    }
}

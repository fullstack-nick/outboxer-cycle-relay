package io.github.fullstacknick.outboxer.central;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("outboxer.central")
public class CentralProperties {

    private long futureClockToleranceSeconds = 300;
    private long haltAfterDatabaseCommitCount;
    private long receiptDelayMillis;
    private int ingressShards = 64;
    private int ingressQueueCapacity = 1_000;
    private MqttEndpoint mqtt = new MqttEndpoint();

    public long getFutureClockToleranceSeconds() {
        return futureClockToleranceSeconds;
    }

    public void setFutureClockToleranceSeconds(long futureClockToleranceSeconds) {
        this.futureClockToleranceSeconds = futureClockToleranceSeconds;
    }

    public long getHaltAfterDatabaseCommitCount() {
        return haltAfterDatabaseCommitCount;
    }

    public void setHaltAfterDatabaseCommitCount(long haltAfterDatabaseCommitCount) {
        this.haltAfterDatabaseCommitCount = haltAfterDatabaseCommitCount;
    }

    public long getReceiptDelayMillis() {
        return receiptDelayMillis;
    }

    public void setReceiptDelayMillis(long receiptDelayMillis) {
        this.receiptDelayMillis = receiptDelayMillis;
    }

    public int getIngressShards() {
        return ingressShards;
    }

    public void setIngressShards(int ingressShards) {
        this.ingressShards = ingressShards;
    }

    public int getIngressQueueCapacity() {
        return ingressQueueCapacity;
    }

    public void setIngressQueueCapacity(int ingressQueueCapacity) {
        this.ingressQueueCapacity = ingressQueueCapacity;
    }

    public MqttEndpoint getMqtt() {
        return mqtt;
    }

    public void setMqtt(MqttEndpoint mqtt) {
        this.mqtt = mqtt;
    }

    public static class MqttEndpoint {
        private String host = "localhost";
        private int port = 11884;
        private String username = "central-uplink";
        private String password = "";
        private String clientId = "outboxer-central-uplink";

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }
    }
}

package com.test.kafka.splunk.integration.conf;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties()
@Component
public class KafkaProperties {

    private java.util.Map<String, String> kafka = new HashMap<>();  // it will store all properties start with app

    public Map<String, String> getKafka() {
        return kafka;
    }

    public void setKafka(Map<String, String> kafka) {
        this.kafka = kafka;
    }
}
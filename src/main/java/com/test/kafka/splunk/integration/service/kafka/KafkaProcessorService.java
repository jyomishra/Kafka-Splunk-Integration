package com.test.kafka.splunk.integration.service.kafka;

import com.test.kafka.splunk.integration.conf.KafkaProperties;
import com.test.kafka.splunk.integration.service.splunk.SplunkForwarderService;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaProcessorService {

    private KafkaConsumer<String, String> myConsumer;

    @Autowired
    private ExecutorService taskExecutor;

    @Autowired
    private SplunkForwarderService splunkForwarderService;

    @Autowired
    private KafkaProperties kafkaProperties;

    @PostConstruct
    public void init() {
        Map<String, String> kafkaPropMap = kafkaProperties.getKafka();
        kafkaPropMap.forEach((k, v) -> System.out.println("Key : " + k + "   Value : " + v));
        this.myConsumer = new KafkaConsumer(kafkaPropMap);
        this.myConsumer.subscribe(Arrays.asList(kafkaPropMap.getOrDefault("topic", "metric-data")));
        //Create a threadpool
        while (true) {
            ConsumerRecords<String, String> records = myConsumer.poll(Duration.ofMillis(3000));
            System.out.println("Records fetch from kafka. record size : " + records.count());
            if(records.count() > 0)
                taskExecutor.submit(new KafkaRecordHandler(records, splunkForwarderService));
        }
    }

    @PreDestroy
    public void shutdown() {
        if (myConsumer != null) {
            myConsumer.close();
        }
        if (taskExecutor != null) {
            taskExecutor.shutdown();
        }
        try {
            if (taskExecutor != null && !taskExecutor.awaitTermination(60, TimeUnit.MILLISECONDS)) {
                taskExecutor.shutdownNow();
            }
        }catch (InterruptedException e) {
            taskExecutor.shutdownNow();
        }
    }

}


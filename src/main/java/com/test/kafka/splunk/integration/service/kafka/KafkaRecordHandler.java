package com.test.kafka.splunk.integration.service.kafka;

import com.test.kafka.splunk.integration.service.splunk.SplunkForwarderService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class KafkaRecordHandler implements Runnable {

    private ConsumerRecords<String, String> records;
    private SplunkForwarderService splunkForwarderService;

    public KafkaRecordHandler(ConsumerRecords<String, String> records, SplunkForwarderService splunkForwarderService) {
        this.records = records;
        this.splunkForwarderService = splunkForwarderService;
    }

    @Override
    public void run() { // this is where further processing happens
        //filter the records
        List<String> listRecord = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            listRecord.add(record.value());
        }
        try {
            splunkForwarderService.putMessage(listRecord);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
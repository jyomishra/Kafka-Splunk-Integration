package com.test.kafka.splunk.integration.service.splunk;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
public class SplunkForwarderService {

    BlockingQueue<List<String>> receivedMessage = new ArrayBlockingQueue<>(1024);

    @Autowired
    ExecutorService taskExecutor;

    public void putMessage(List<String> listStr) throws InterruptedException{
        receivedMessage.put(listStr);
    }

    public List<String> readMessage() {
        List<List<String>> output = new ArrayList<>();
        receivedMessage.drainTo(output);
        return output.stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    @PostConstruct
    public void init() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate((new Runnable() {
            @Override
            public void run() {
                List<String> messages = readMessage();
                System.out.println("Received data : " + messages);
            }
        }), 1, 15, TimeUnit.SECONDS );
    }
}
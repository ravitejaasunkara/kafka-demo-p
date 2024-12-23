package com.kafka.producer_demo.service;

import com.kafka.producer_demo.models.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    @Autowired
    private KafkaTemplate<String, Object> template;

    @Value("${kafka.topic.name}")
    private String topicName;

    public void sendMessageToKafka(String message) {
        CompletableFuture<SendResult<String, Object>> res = template.send(topicName,1,null, message);
        res.whenComplete((result,ex) -> {
           if(ex == null) {
               logger.info("Message send successfully with offset:{} to topic:{}",result.getRecordMetadata().offset(),result.getProducerRecord().topic());
           } else {
               logger.error("Unable to send message due to:",ex);
           }
        });
    }

    public void sendCustomer(Customer customer) {
        CompletableFuture<SendResult<String, Object>> result = template.send(topicName, customer);
        result.whenComplete((res,ex) -> {
            if(ex == null) {
                logger.info("Message send successfully with offset:{} to topic:{}",res.getRecordMetadata().offset(),res.getProducerRecord().topic());
            } else {
                logger.error("Unable to send message due to:",ex);
            }
        });
    }
}

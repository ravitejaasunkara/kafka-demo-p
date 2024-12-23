package com.kafka.producer_demo.controller;

import com.kafka.producer_demo.models.Customer;
import com.kafka.producer_demo.service.MessageProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("message")
public class MessageController {
    @Autowired
    MessageProducer messageProducer;

    @GetMapping("/send/{message}")
    public ResponseEntity<String> sendMessage(@PathVariable("message") String message) {
        try{
            for (int i = 0; i < 100; i++) {
                messageProducer.sendMessageToKafka(message);
            }
            return ResponseEntity.status(HttpStatus.OK).body("Message sent successfully");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @PostMapping("/publishCustomer")
    public void sendCustomer(@RequestBody Customer customer) {
        messageProducer.sendCustomer(customer);
    }
}

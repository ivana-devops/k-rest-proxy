package com.example.krestproxy.controller;

import com.example.krestproxy.dto.MessageDto;
import com.example.krestproxy.service.KafkaMessageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/messages")
public class MessageController {

    private final KafkaMessageService kafkaMessageService;

    @Autowired
    public MessageController(KafkaMessageService kafkaMessageService) {
        this.kafkaMessageService = kafkaMessageService;
    }

    @GetMapping("/{topic}")
    public ResponseEntity<List<MessageDto>> getMessages(
            @PathVariable String topic,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime) {

        if (startTime.isAfter(endTime)) {
            return ResponseEntity.badRequest().build();
        }

        var messages = kafkaMessageService.getMessages(topic, startTime, endTime);
        return ResponseEntity.ok(messages);
    }

    @GetMapping("/{topic}/filter")
    public ResponseEntity<List<MessageDto>> getMessagesWithExecId(
            @PathVariable String topic,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime,
            @RequestParam String execId) {

        if (startTime.isAfter(endTime)) {
            return ResponseEntity.badRequest().build();
        }

        var messages = kafkaMessageService.getMessagesWithExecId(topic, startTime, endTime, execId);
        return ResponseEntity.ok(messages);
    }

    @GetMapping("/filter")
    public ResponseEntity<List<MessageDto>> getMessagesFromTopics(
            @RequestParam List<String> topics,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant startTime,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant endTime,
            @RequestParam(required = false) String execId) {

        if (startTime.isAfter(endTime)) {
            return ResponseEntity.badRequest().build();
        }

        var messages = kafkaMessageService.getMessagesFromTopics(topics, startTime, endTime, execId);
        return ResponseEntity.ok(messages);
    }
}

package com.example.krestproxy.dto;

public record MessageDto(String topicName, String content, long timestamp, int partition, long offset) {}

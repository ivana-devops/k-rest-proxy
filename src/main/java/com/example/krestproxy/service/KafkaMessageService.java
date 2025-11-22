package com.example.krestproxy.service;

import com.example.krestproxy.dto.MessageDto;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.pool2.ObjectPool;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KafkaMessageService {

    private static final String EXEC_IDS_TOPIC = "execids";
    private final ObjectPool<Consumer<Object, Object>> consumerPool;
    private final Map<String, ExecTime> execTimeCache = new ConcurrentHashMap<>();

    @Autowired
    public KafkaMessageService(ObjectPool<Consumer<Object, Object>> consumerPool) {
        this.consumerPool = consumerPool;
    }

    public List<MessageDto> getMessagesForExecution(List<String> topics, String execId) {
        var times = findExecutionTimes(execId);
        return getMessagesInternal(topics, times.start(), times.end(), execId);
    }

    private record ExecTime(Instant start, Instant end) {
    }

    private ExecTime findExecutionTimes(String execId) {
        if (execTimeCache.containsKey(execId)) {
            return execTimeCache.get(execId);
        }

        Consumer<Object, Object> consumer = null;
        try {
            consumer = consumerPool.borrowObject();
            var topicPartition = new TopicPartition(EXEC_IDS_TOPIC, 0);
            consumer.assign(List.of(topicPartition));
            consumer.seekToBeginning(List.of(topicPartition));

            Instant startTime = null;
            Instant endTime = null;

            // Assuming "few days" retention isn't massive, but we should be careful.
            // We scan until we find both or reach end.
            while (startTime == null || endTime == null) {
                var records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    break;
                }

                for (var record : records) {
                    String keyStr = record.key().toString();
                    if (execId.equals(keyStr)) {
                        String valStr = record.value().toString();
                        if ("start".equals(valStr)) {
                            startTime = Instant.ofEpochMilli(record.timestamp());
                        } else if ("end".equals(valStr)) {
                            endTime = Instant.ofEpochMilli(record.timestamp());
                        }
                    }
                }
            }

            if (startTime == null || endTime == null) {
                throw new RuntimeException("Could not find start and/or end time for execution ID: " + execId);
            }

            var execTime = new ExecTime(startTime, endTime);
            execTimeCache.put(execId, execTime);
            return execTime;

        } catch (Exception e) {
            if (e instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException("Error scanning execids topic", e);
        } finally {
            if (consumer != null) {
                try {
                    consumerPool.returnObject(consumer);
                } catch (Exception e) {
                    // Log error
                }
            }
        }
    }

    public List<MessageDto> getMessages(String topic, Instant startTime, Instant endTime) {
        return getMessagesInternal(List.of(topic), startTime, endTime, null);
    }

    public List<MessageDto> getMessagesWithExecId(String topic, Instant startTime, Instant endTime, String execId) {
        return getMessagesInternal(List.of(topic), startTime, endTime, execId);
    }

    public List<MessageDto> getMessagesFromTopics(List<String> topics, Instant startTime, Instant endTime,
            String execId) {
        return getMessagesInternal(topics, startTime, endTime, execId);
    }

    private List<MessageDto> getMessagesInternal(java.util.Collection<String> topics, Instant startTime,
            Instant endTime, String execId) {
        Consumer<Object, Object> consumer = null;
        try {
            consumer = consumerPool.borrowObject();

            var partitions = new ArrayList<TopicPartition>();
            for (String topic : topics) {
                var partitionInfos = consumer.partitionsFor(topic);
                if (partitionInfos != null) {
                    partitions.addAll(partitionInfos.stream()
                            .map(pi -> new TopicPartition(topic, pi.partition()))
                            .toList());
                }
            }

            consumer.assign(partitions);

            // Find offsets for start time
            var timestampsToSearch = new HashMap<TopicPartition, Long>();
            for (var partition : partitions) {
                timestampsToSearch.put(partition, startTime.toEpochMilli());
            }

            var startOffsets = consumer.offsetsForTimes(timestampsToSearch);

            // Find offsets for end time
            var endTimestampsToSearch = new HashMap<TopicPartition, Long>();
            for (var partition : partitions) {
                endTimestampsToSearch.put(partition, endTime.toEpochMilli());
            }

            var endOffsets = consumer.offsetsForTimes(endTimestampsToSearch);

            var messages = new ArrayList<MessageDto>();

            for (var partition : partitions) {
                var startOffset = startOffsets.get(partition);
                var endOffset = endOffsets.get(partition);

                if (startOffset != null) {
                    consumer.seek(partition, startOffset.offset());

                    var keepReading = true;
                    while (keepReading) {
                        var records = consumer.poll(Duration.ofMillis(100));
                        if (records.isEmpty()) {
                            break;
                        }

                        for (var record : records.records(partition)) {
                            if (record.timestamp() >= startTime.toEpochMilli()
                                    && record.timestamp() <= endTime.toEpochMilli()) {

                                var match = true;
                                if (execId != null) {
                                    if (record.key() instanceof GenericRecord keyRecord) {
                                        var execIdObj = keyRecord.get("exec_id");
                                        if (execIdObj == null || !execIdObj.toString().equals(execId)) {
                                            match = false;
                                        }
                                    } else {
                                        // If key is not GenericRecord, we can't check exec_id, so mismatch
                                        match = false;
                                    }
                                }

                                if (match) {
                                    String content = switch (record.value()) {
                                        case GenericRecord genericRecord -> convertAvroToJson(genericRecord);
                                        case null -> null;
                                        case Object o -> o.toString();
                                    };

                                    messages.add(new MessageDto(
                                            record.topic(),
                                            content,
                                            record.timestamp(),
                                            record.partition(),
                                            record.offset()));
                                }

                            } else if (record.timestamp() > endTime.toEpochMilli()) {
                                keepReading = false;
                                break;
                            }

                            // Optimization: if we have a target end offset, we can check position.
                            if (endOffset != null && record.offset() >= endOffset.offset()) {
                                keepReading = false;
                                break;
                            }
                        }

                        // Safety break if we reached end of partition
                        if (endOffset != null && consumer.position(partition) >= endOffset.offset()) {
                            keepReading = false;
                        }
                    }
                }
            }
            return messages;
        } catch (Exception e) {
            throw new RuntimeException("Error fetching messages from Kafka", e);
        } finally {
            if (consumer != null) {
                try {
                    consumerPool.returnObject(consumer);
                } catch (Exception e) {
                    // Log error returning to pool
                }
            }
        }
    }

    private String convertAvroToJson(GenericRecord record) {
        try {
            var outputStream = new java.io.ByteArrayOutputStream();
            var jsonEncoder = org.apache.avro.io.EncoderFactory.get()
                    .jsonEncoder(record.getSchema(), outputStream);
            var writer = new org.apache.avro.generic.GenericDatumWriter<GenericRecord>(
                    record.getSchema());
            writer.write(record, jsonEncoder);
            jsonEncoder.flush();
            return outputStream.toString();
        } catch (java.io.IOException e) {
            throw new RuntimeException("Error converting Avro to JSON", e);
        }
    }
}

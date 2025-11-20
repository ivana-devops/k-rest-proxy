package com.example.krestproxy.service;

import org.apache.commons.pool2.ObjectPool;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaMessageServiceTest {

        @Mock
        private ObjectPool<Consumer<String, Object>> consumerPool; // Changed from ConsumerFactory

        @Mock
        private Consumer<String, Object> consumer;

        private KafkaMessageService kafkaMessageService;

        @BeforeEach
        void setUp() throws Exception { // Added throws Exception
                kafkaMessageService = new KafkaMessageService(consumerPool); // Changed to use consumerPool
        }

        @Test
        void getMessages_shouldReturnMessagesWithinTimeRange() throws Exception { // Added throws Exception
                String topic = "test-topic";
                Instant startTime = Instant.parse("2023-01-01T10:00:00Z");
                Instant endTime = Instant.parse("2023-01-01T10:05:00Z");
                TopicPartition partition0 = new TopicPartition(topic, 0);

                when(consumerPool.borrowObject()).thenReturn(consumer); // Changed from consumerFactory.createConsumer()
                when(consumer.partitionsFor(topic)).thenReturn(
                                Collections.singletonList(
                                                new org.apache.kafka.common.PartitionInfo(topic, 0, null, null, null)));

                // Mock offsetsForTimes
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = new HashMap<>();
                startOffsets.put(partition0, new OffsetAndTimestamp(0L, startTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == startTime.toEpochMilli())))
                                .thenReturn(startOffsets);

                Map<TopicPartition, OffsetAndTimestamp> endOffsets = new HashMap<>();
                endOffsets.put(partition0, new OffsetAndTimestamp(10L, endTime.toEpochMilli()));
                when(consumer.offsetsForTimes(
                                argThat(map -> map != null && map.containsKey(partition0)
                                                && map.get(partition0) == endTime.toEpochMilli())))
                                .thenReturn(endOffsets);

                // Mock poll
                ConsumerRecord<String, Object> record1 = new ConsumerRecord<>(topic, 0, 0L, startTime.toEpochMilli(),
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg1",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
                ConsumerRecord<String, Object> record2 = new ConsumerRecord<>(topic, 0, 5L,
                                startTime.toEpochMilli() + 1000,
                                org.apache.kafka.common.record.TimestampType.CREATE_TIME, 0, 0, "key", "msg2",
                                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

                Map<TopicPartition, List<ConsumerRecord<String, Object>>> recordsMap = new HashMap<>();
                recordsMap.put(partition0, Arrays.asList(record1, record2));
                ConsumerRecords<String, Object> records = new ConsumerRecords<>(recordsMap);

                when(consumer.poll(any())).thenReturn(records)
                                .thenReturn(new ConsumerRecords<>(Collections.emptyMap()));

                List<com.example.krestproxy.dto.MessageDto> messages = kafkaMessageService.getMessages(topic, startTime,
                                endTime);

                assertEquals(2, messages.size());
                assertEquals("msg1", messages.get(0).getContent());
                assertEquals("msg2", messages.get(1).getContent());

                verify(consumer).assign(Collections.singletonList(partition0));
                verify(consumer).seek(partition0, 0L);
                verify(consumerPool).returnObject(consumer); // Added verification for returning object to pool
        }
}

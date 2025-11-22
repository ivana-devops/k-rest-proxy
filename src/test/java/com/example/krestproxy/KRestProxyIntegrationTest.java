package com.example.krestproxy;

import com.example.krestproxy.dto.MessageDto;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
public class KRestProxyIntegrationTest {

        private static final Network NETWORK = Network.newNetwork();

        @Container
        static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"))
                        .withNetwork(NETWORK);

        @Container
        static final GenericContainer<?> schemaRegistry = new GenericContainer<>(
                        DockerImageName.parse("confluentinc/cp-schema-registry:7.6.0"))
                        .withNetwork(NETWORK)
                        .withExposedPorts(8081)
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                        .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                                        "PLAINTEXT://" + kafka.getNetworkAliases().get(0) + ":9092")
                        .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
                        .dependsOn(kafka)
                        .withStartupTimeout(java.time.Duration.ofMinutes(5));

        @DynamicPropertySource
        static void registerProperties(DynamicPropertyRegistry registry) {
                registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
                registry.add("spring.kafka.properties.schema.registry.url",
                                () -> "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));
                registry.add("spring.kafka.consumer.properties.schema.registry.url",
                                () -> "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));
        }

        @LocalServerPort
        private int port;

        @Test
        void testGetMessages() throws Exception {
                String topic = "test-integration-topic";
                String keySchemaString = "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"exec_id\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}";
                Schema keySchema = new Schema.Parser().parse(keySchemaString);
                GenericRecord key = new GenericData.Record(keySchema);
                key.put("version", "v1");
                key.put("exec_id", "exec-1");
                key.put("timestamp", 1000L);

                String valueSchemaString = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
                Schema valueSchema = new Schema.Parser().parse(valueSchemaString);
                GenericRecord user = new GenericData.Record(valueSchema);
                user.put("name", "Alice");
                user.put("age", 30);

                // Produce message
                Map<String, Object> props = new HashMap<>();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                props.put("schema.registry.url",
                                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));

                ProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(props);
                KafkaTemplate<Object, Object> template = new KafkaTemplate<>(producerFactory);
                template.send(topic, key, user).get();

                // Wait a bit for message to be available
                Thread.sleep(2000);

                // Call API
                HttpHeaders headers = new HttpHeaders();
                headers.set("X-API-KEY", "secret-api-key");
                HttpEntity<String> entity = new HttpEntity<>(headers);

                String url = "https://localhost:" + port + "/api/v1/messages/" + topic +
                                "?startTime=" + Instant.now().minusSeconds(60).toString() +
                                "&endTime=" + Instant.now().plusSeconds(60).toString();

                org.springframework.web.client.RestTemplate looseRestTemplate = createInsecureRestTemplate();

                ResponseEntity<List<MessageDto>> response = looseRestTemplate.exchange(
                                url,
                                HttpMethod.GET,
                                entity,
                                new ParameterizedTypeReference<List<MessageDto>>() {
                                });

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                List<MessageDto> messages = response.getBody();
                assertThat(messages).isNotEmpty();
                assertThat(messages.get(0).content()).contains("\"name\":\"Alice\"");
                assertThat(messages.get(0).content()).contains("\"age\":30");
        }

        @Test
        void testGetMessagesWithExecId() throws Exception {
                String topic = "test-integration-topic-filter";

                // Define schemas
                String keySchemaString = "{\"type\":\"record\",\"name\":\"Key\",\"fields\":[{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"exec_id\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"}]}";
                Schema keySchema = new Schema.Parser().parse(keySchemaString);

                String valueSchemaString = "{\"type\":\"record\",\"name\":\"Value\",\"fields\":[{\"name\":\"data\",\"type\":\"string\"}]}";
                Schema valueSchema = new Schema.Parser().parse(valueSchemaString);

                // Produce messages
                Map<String, Object> props = new HashMap<>();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                props.put("schema.registry.url",
                                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));

                ProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(props);
                KafkaTemplate<Object, Object> template = new KafkaTemplate<>(producerFactory);

                // Message 1: Matches exec_id
                GenericRecord key1 = new GenericData.Record(keySchema);
                key1.put("version", "v1");
                key1.put("exec_id", "exec-target");
                key1.put("timestamp", System.currentTimeMillis());

                GenericRecord val1 = new GenericData.Record(valueSchema);
                val1.put("data", "match");

                template.send(topic, key1, val1).get();

                // Message 2: Different exec_id
                GenericRecord key2 = new GenericData.Record(keySchema);
                key2.put("version", "v1");
                key2.put("exec_id", "exec-other");
                key2.put("timestamp", System.currentTimeMillis());

                GenericRecord val2 = new GenericData.Record(valueSchema);
                val2.put("data", "no-match");

                template.send(topic, key2, val2).get();

                // Wait a bit
                Thread.sleep(2000);

                // Call API
                HttpHeaders headers = new HttpHeaders();
                headers.set("X-API-KEY", "secret-api-key");
                HttpEntity<String> entity = new HttpEntity<>(headers);

                String url = "https://localhost:" + port + "/api/v1/messages/" + topic + "/filter" +
                                "?startTime=" + Instant.now().minusSeconds(60).toString() +
                                "&endTime=" + Instant.now().plusSeconds(60).toString() +
                                "&execId=exec-target";

                // Use loose RestTemplate
                org.springframework.web.client.RestTemplate looseRestTemplate = createInsecureRestTemplate();

                ResponseEntity<List<MessageDto>> response = looseRestTemplate.exchange(
                                url,
                                HttpMethod.GET,
                                entity,
                                new ParameterizedTypeReference<List<MessageDto>>() {
                                });

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                List<MessageDto> messages = response.getBody();
                assertThat(messages).hasSize(1);
                assertThat(messages.get(0).content()).contains("match");
                assertThat(messages.get(0).content()).doesNotContain("no-match");
        }

        private org.springframework.web.client.RestTemplate createInsecureRestTemplate() throws Exception {
                javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[] {
                                new javax.net.ssl.X509TrustManager() {
                                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                                                return null;
                                        }

                                        public void checkClientTrusted(java.security.cert.X509Certificate[] certs,
                                                        String authType) {
                                        }

                                        public void checkServerTrusted(java.security.cert.X509Certificate[] certs,
                                                        String authType) {
                                        }
                                }
                };

                javax.net.ssl.SSLContext sc = javax.net.ssl.SSLContext.getInstance("TLS");
                sc.init(null, trustAllCerts, new java.security.SecureRandom());
                javax.net.ssl.HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
                javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

                return new org.springframework.web.client.RestTemplate();
        }
}

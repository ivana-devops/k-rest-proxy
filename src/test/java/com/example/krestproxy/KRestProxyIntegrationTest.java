package com.example.krestproxy;

import com.example.krestproxy.dto.MessageDto;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
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
                String schemaString = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
                Schema schema = new Schema.Parser().parse(schemaString);
                GenericRecord user = new GenericData.Record(schema);
                user.put("name", "Alice");
                user.put("age", 30);

                // Produce message
                Map<String, Object> props = new HashMap<>();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                props.put("schema.registry.url",
                                "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081));

                ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(props);
                KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
                template.send(topic, "user1", user).get();

                // Wait a bit for message to be available
                Thread.sleep(2000);

                // Call API
                HttpHeaders headers = new HttpHeaders();
                headers.set("X-API-KEY", "secret-api-key");
                HttpEntity<String> entity = new HttpEntity<>(headers);

                String url = "https://localhost:" + port + "/api/v1/messages/" + topic +
                                "?startTime=" + Instant.now().minusSeconds(60).toString() +
                                "&endTime=" + Instant.now().plusSeconds(60).toString();

                // Note: TestRestTemplate might complain about self-signed cert if not
                // configured,
                // but let's try. If it fails, we might need to configure SSL context or use a
                // custom RestTemplate.
                // For simplicity in this test, we might assume the server is running with SSL
                // but TestRestTemplate
                // is configured to trust it or we might need to disable SSL verification for
                // the test client.
                // However, Spring Boot's TestRestTemplate is usually auto-configured.
                // Let's see. Actually, since we generated a self-signed cert, we might hit
                // issues.
                // A common workaround for tests is to use a custom request factory that trusts
                // all certs.
                // But let's write the test first.

                // Create SSL-ignoring RestTemplate
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

                org.springframework.web.client.RestTemplate looseRestTemplate = new org.springframework.web.client.RestTemplate();

                // We can't easily set SSLContext on SimpleClientHttpRequestFactory directly
                // without reflection or subclassing if we want to avoid global state,
                // but setting default SSLSocketFactory affects HttpsURLConnection which
                // SimpleClientHttpRequestFactory uses.
                // A better way is using Apache HttpClient if available, but let's try setting
                // global default for the test execution.
                // Alternatively, we can use HttpComponentsClientHttpRequestFactory if we add
                // httpclient dependency.

                // Let's try a cleaner approach using a custom request factory if possible, or
                // just the global setting which is hacky but works for tests.

                // Actually, since we are using Spring Boot, we can use RestTemplateBuilder if
                // we had it injected, but we can also just use the global SSL context hack for
                // this test method.

                ResponseEntity<List<MessageDto>> response = looseRestTemplate.exchange(
                                url,
                                HttpMethod.GET,
                                entity,
                                new ParameterizedTypeReference<List<MessageDto>>() {
                                });

                assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
                List<MessageDto> messages = response.getBody();
                assertThat(messages).isNotEmpty();
                assertThat(messages.get(0).getContent()).contains("\"name\":\"Alice\"");
                assertThat(messages.get(0).getContent()).contains("\"age\":30");
        }
}

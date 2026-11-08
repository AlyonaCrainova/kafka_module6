package org.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        //получаем из переменной среды адрес брокера для первого подключения
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            System.err.println("Ошибка: переменная KAFKA_BOOTSTRAP_SERVERS не установлена!");
            return;
        }

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // устанавливаем адрес для первого подключения
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // класс для сериализации ключа сообщения
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // класс для сериализации тела сообщения

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, System.getenv().getOrDefault("SSL_TRUSTSTORE_LOCATION", "kafka-0-creds/kafka.kafka-0.truststore.jks")); // Truststore
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, System.getenv().getOrDefault("SSL_TRUSTSTORE_PASSWORD", "551288d")); // Truststore password
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Отключение проверки hostname

        // Конфигурация SASL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"producer\" password=\"password\";");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-1", "key", "SASL/PLAIN");
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Сообщение успешно отправлено в Kafka: " + metadata.toString());
                    System.out.println(metadata);
                } else {
                    logger.error("Ошибка при отправке сообщения", exception);
                }
            });
            producer.flush();
        } catch (Throwable e) {
            logger.error("Ошибка в Kafka Producer", e);
        }

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-2", "key", "SASL/PLAIN");
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Сообщение успешно отправлено в Kafka: " + metadata.toString());
                    System.out.println(metadata);
                } else {
                    logger.error("Ошибка при отправке сообщения", exception);
                }
            });
            producer.flush();
        } catch (Throwable e) {
            logger.error("Ошибка в Kafka Producer", e);
        }
    }
}
package com.github.zainzin;

import com.github.javafaker.Name;
import com.github.zainzin.avro.Customer;
import com.github.zainzin.utils.SchemaProcessor;
import com.github.zainzin.utils.TopicUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Calendar;
import java.util.Properties;

public class CustomerProducer {
    public static void main(String[] args) {
        final String topic = "customer-topic";

        TopicUtils.createTopic(topic, 1, 1);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "CustomerStream");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());


        try (Producer<String, byte[]> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 100; i++) {
                Name name = EventGenerator.generateFullName();
                Customer customer = new Customer(1 + i, name.username(), EventGenerator.generateEmail(name.username()), EventGenerator.generatePhoneNumber().cellPhone(),
                        name.firstName(), name.lastName(), name.nameWithMiddle(), EventGenerator.generateGender(), EventGenerator.generateDate(1930, 2010),
                        EventGenerator.generateDate(2010, Calendar.getInstance().get(Calendar.YEAR)), EventGenerator.randBetween(1, 10), EventGenerator.generateIp());
                ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topic, String.valueOf(customer.getId() + i), SchemaProcessor.serializeCustomer(customer));
                producer.send(producerRecord);
                producer.flush();
                Thread.sleep(500);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

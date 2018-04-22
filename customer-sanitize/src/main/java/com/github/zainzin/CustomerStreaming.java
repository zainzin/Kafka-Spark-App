package com.github.zainzin;

import com.github.zainzin.avro.Customer;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CustomerStreaming {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put("metadata.broker.list", "127.0.0.1:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerStream");

        Set<String> topics = Collections.singleton("customer-topic");

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("CustomerStream");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(jssc,
                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.map(stringTuple2 -> deserializeCustomer(stringTuple2._2))
                    .filter(customer -> customer.getPreviousLogins() <= 2)
                     .foreach(customer -> System.out.println(customer.toString()));
        });


        jssc.start();
        jssc.awaitTermination();
    }

    private static Customer deserializeCustomer(byte[] customer) throws IOException {
        SpecificDatumReader<Customer> reader = new SpecificDatumReader<>(Customer.getClassSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(customer, null);
        return reader.read(null, decoder);
    }
}

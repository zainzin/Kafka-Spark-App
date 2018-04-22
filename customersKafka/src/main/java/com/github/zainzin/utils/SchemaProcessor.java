package com.github.zainzin.utils;

import com.github.zainzin.avro.Customer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SchemaProcessor {

    public static byte[] serializeCustomer(Customer customer) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        SpecificDatumWriter<Customer> specificDatumWriter = new SpecificDatumWriter<>(Customer.getClassSchema());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        specificDatumWriter.write(customer, encoder);
        encoder.flush();
        byteArrayOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }

    public static Customer deserializeCustomer(byte[] customer) throws IOException {
        SpecificDatumReader<Customer> reader = new SpecificDatumReader<>(Customer.getClassSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(customer, null);
        return reader.read(null, decoder);
    }
}

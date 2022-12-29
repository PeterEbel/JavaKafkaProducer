package org.example;

import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.Properties;


class Customer {

    String id;
    String entityID;
    String customerNumber;
    String validFrom;
    String validTo;
    String genderCode;
    String lastName;
    String firstName;
    String birthDate;
    String countryCode;
    String postalCode;
    String city;
    String street;
    String partitionDate;

    public Customer() {
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setEntityID(String entityID) {
        this.entityID = entityID;
    }

    public void setCustomerNumber(String customerNumber) {
        this.customerNumber = customerNumber;
    }

    public void setValidFrom(String validFrom) {
        this.validFrom = validFrom;
    }

    public void setValidTo(String validTo) {
        this.validTo = validTo;
    }

    public void setGenderCode(String genderCode) {
        this.genderCode = genderCode;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setBirthDate(String birthDate) {
        this.birthDate = birthDate;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public void setPartitionDate(String partitionDate) {
        this.partitionDate = partitionDate;
    }

    public String getId() {
        return id;
    }

    public String getEntityID() {
        return entityID;
    }

    public String getCustomerNumber() {
        return customerNumber;
    }

    public String getValidFrom() {
        return validFrom;
    }

    public String getValidTo() {
        return validTo;
    }

    public String getGenderCode() {
        return genderCode;
    }

    public String getLastName() {
        return lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getBirthDate() {
        return birthDate;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public String getCity() {
        return city;
    }

    public String getStreet() {
        return street;
    }

    public String getPartitionDate() {
        return partitionDate;
    }

    public String toString(String separator) {
        return (
                this.id + separator +
                this.entityID + separator +
                this.customerNumber + separator +
                this.validFrom + separator +
                this.validTo + separator +
                this.genderCode + separator +
                this.lastName + separator +
                this.firstName + separator +
                this.birthDate + separator +
                this.countryCode + separator +
                this.postalCode + separator +
                this.city + separator +
                this.street + separator +
                this.partitionDate
        );
    }
}

public class JavaKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(JavaKafkaProducer.class);
    private static final String TOPIC = "localhost.shop.customers";
    private static final String AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private static final String KAFKA_SERVER_ADDRESS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_SERVER_URL = "http://localhost:8081";

    public static void main(String[] args) throws FileNotFoundException, JsonMappingException {

        log.info("Starting Kafka Producer to show Near Real Time Ingestion using Apache Flink");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_ADDRESS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AVRO_SERIALIZER_CLASS);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AVRO_SERIALIZER_CLASS);
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_SERVER_URL);
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");

        // Schema generation for our Customer class
        final AvroMapper avroMapper = new AvroMapper();
        final AvroSchema avroSchema = avroMapper.schemaFor(Customer.class);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties);
        File customers = new File("./src/main/resources/data/customers.csv");
        final Scanner scanner = new Scanner(customers);
        final Customer customer = new Customer();

        while (scanner.hasNextLine()) {
            String customerRecord = scanner.nextLine();
            String[] customerDetail = customerRecord.split("\\|");
            customer.id = customerDetail[0];
            customer.entityID = customerDetail[1];
            customer.customerNumber = customerDetail[2];
            customer.validFrom = customerDetail[3];
            customer.validTo = customerDetail[4];
            customer.genderCode = customerDetail[5];
            customer.lastName = customerDetail[6];
            customer.firstName = customerDetail[7];
            customer.birthDate = customerDetail[8];
            customer.countryCode = customerDetail[9];
            customer.postalCode = customerDetail[10];
            customer.city = customerDetail[11];
            customer.street = customerDetail[12];
            customer.partitionDate = customerDetail[13];

            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema.getAvroSchema());
            recordBuilder.set("id", customer.id);
            recordBuilder.set("entityID", customer.entityID);
            recordBuilder.set("customerNumber", customer.customerNumber);
            recordBuilder.set("validFrom", customer.validFrom);
            recordBuilder.set("validTo", customer.validTo);
            recordBuilder.set("genderCode", customer.genderCode);
            recordBuilder.set("lastName", customer.lastName);
            recordBuilder.set("firstName", customer.firstName);
            recordBuilder.set("birthDate", customer.birthDate);
            recordBuilder.set("countryCode", customer.countryCode);
            recordBuilder.set("postalCode", customer.postalCode);
            recordBuilder.set("city", customer.city);
            recordBuilder.set("street", customer.street);
            recordBuilder.set("partitionDate", customer.partitionDate);

            final GenericRecord genericRecord = recordBuilder.build();
            final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(TOPIC, customer.id, genericRecord);
            producer.send(producerRecord);
        }

        producer.flush();
        producer.close();

    }
}
package br.com.caminha.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        var service = new KafkaService("ECOMMERCE_SEND_EMAIL",emailService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("--------------------------------------");
        System.out.println("Sending email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try{
            Thread.sleep(1000);
        }catch (InterruptedException ex){
            ex.printStackTrace();
        }
        System.out.println("Order processed");

    }


}




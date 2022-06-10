package br.com.caminha.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()){
            var value = "1234,12314,1231243";
            dispatcher.send("ECOMMERCE_NEW_ORDER", value, value);

            var email = "Welcome! We are processing your order";
            dispatcher.send("ECOMMERCE_SEND_EMAIL", email, email);
        }

    }


}

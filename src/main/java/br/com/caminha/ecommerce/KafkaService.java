package br.com.caminha.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService {

    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;
    //como a interface consumerfunction possui apenas um metodo a ser implementado o java e inteligente o suficiente para entendeder
    //que o method reference passado esta implementando este metodo da interface, sendo que o metodo passado recebe os mesmo parametros
    public KafkaService(String groupId, String topic, ConsumerFunction parse) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<String, String>(properties(groupId));
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void run(){
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println("Registros encontrados");
                for(var record : records){
                    parse.consume(record);
                }
            }
        }
    }

    private static Properties properties(String groupId){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.1.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //cada consumer deve pertencer a um group para segregar a entrega das mensagens
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID().toString());
        //pega as mensagens de 1 em 1, evitando duplicaçao e tornando mais tolerante a mudanças no kafka
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}

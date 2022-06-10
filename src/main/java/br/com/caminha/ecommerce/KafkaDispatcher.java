package br.com.caminha.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {

    private final KafkaProducer<String, String> producer;
    public KafkaDispatcher(){
        this.producer = new KafkaProducer<String, String>(properties());
    }




    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<String, String>(topic, key, value);
        // o .send() por default retorna um future, com o .get() é possivel tornar o retorno sincrono
        //o metodo send recebe também uma interface que tem apenas um metodo sendo possivel
        //passar uma lambda expression implementando o metodo dessa interface que recebe um metadata
        // e uma exception, sendo assim e possivel passar uma açao para ser executada em caso de sucesso ou falha
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition" + data.partition() + "/offset " + data.offset() + "/timestamp " + data.timestamp());
        };
        producer.send(record, callback).get();
    }

    @Override
    public void close(){
       producer.close();
    }


    private static Properties properties(){
        var properties = new Properties();
        //Passar o endereço de onde o kafka está rodando
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.1.1:9092");
        // Passar um serializer de strings para bytes para a key e para o value, pois a chave e valor e em string
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}

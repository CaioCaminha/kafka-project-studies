package br.com.caminha.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "1234,12314,1231243";
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);
        // o .send() por default retorna um future, com o .get() é possivel tornar o retorno sincrono
        //o metodo send recebe também uma interface que tem apenas um metodo sendo possivel
        //passar uma lambda expression implementando o metodo dessa interface que recebe um metadata
        // e uma exception, sendo assim e possivel passar uma açao para ser executada em caso de sucesso ou falha
        producer.send(record, (data, ex) -> {
            if(ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition" + data.partition() + "/offset " + data.offset() + "/timestamp " + data.timestamp());
        }).get();
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

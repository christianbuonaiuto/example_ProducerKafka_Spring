package com.demo.kafka.springbootwithkafka.config;
//Serve a definire dentro kafka la comunicazone trmaite formato JSON

import com.demo.kafka.springbootwithkafka.model.User;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    //configurazione string
    @Bean
    public ProducerFactory<String,String> producerFactory(){

        Map<String,Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String,String> kafkaTemplate = new KafkaTemplate<>(producerFactory());

        kafkaTemplate.setProducerListener(new ProducerListener<String, String>() {
            @Override
            public void onSuccess(ProducerRecord<String, String> producerRecord, RecordMetadata recordMetadata) {
                System.out.println("ACK from ProducerListener message:"+producerRecord.value()+" offset:"+recordMetadata.offset());
            }
        });

        return kafkaTemplate;
    }

    //configurazione messaggi json
    @Bean
    public ProducerFactory<String,User> producerFactoryJson(){

        Map<String,Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    /*
    METODO CONFIGURAZIONE CLASSICA
    @Bean
    public KafkaTemplate<String, User> kafkaTemplate() { return new KafkaTemplate<>(producerFactory()); }
    */

    @Bean //METODO CONFIGURAZIONE CON CALLBACK
    public KafkaTemplate<String, User> kafkaTemplateJson() {
        KafkaTemplate<String,User> kafkaTemplate = new KafkaTemplate<>(producerFactoryJson());

        kafkaTemplate.setProducerListener(new ProducerListener<String, User>() {
            @Override
            public void onSuccess(ProducerRecord<String, User> producerRecord, RecordMetadata recordMetadata) {
                System.out.println("ACK from ProducerListener JSON message:"+producerRecord.value()+" offset:"+recordMetadata.offset());
            }
        });

        return kafkaTemplate;
    }


}

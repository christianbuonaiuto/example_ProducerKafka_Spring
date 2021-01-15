package com.demo.kafka.springbootwithkafka.resource;

import com.demo.kafka.springbootwithkafka.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("kafka")
public class KafkaResource {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplateJson;

    private static final String TOPIC = "TOPIC_TEST";
    private static final String TOPIC_JSON = "TOPIC_TEST_json";

    @GetMapping("/publish/{name}")
    public String post(@PathVariable("name") final String name) {

        kafkaTemplate.send(TOPIC,name);

        return "Published successfully";
    }

    @GetMapping("/publish_json_url/{name}")
    public String postJsonUrl(@PathVariable("name") final String name) {

        kafkaTemplateJson.send(TOPIC_JSON,new User(name,"tecnology",1200L));

        return "Published successfully";
    }

    @PostMapping("/publish_json")
    public String postJson(@RequestBody User user) {

        kafkaTemplateJson.send(TOPIC_JSON,user);

        return "Published successfully";
    }
    /*
    CONFIGURAZIONE PUBBLICAZIONE MESSAGGIO CON CALLBACK DI VERIFICA
    @GetMapping("/publish_callback/{name}")
    public String postWithCallBack(@PathVariable("name") final String name) {

        User message = new User(name,"tecnology",1200L);

        ListenableFuture<SendResult<String,User>> future = kafkaTemplate.send(TOPIC,message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {
            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("Unable to deliver message:");
                System.out.println(message.toString());
                System.out.println(throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, User> result) {
                System.out.println("Message delivered:");
                System.out.println(message.toString());
                System.out.println("With offset:"+ result.getRecordMetadata().offset());
            }
        });

        return "Published";
    }
    */
}

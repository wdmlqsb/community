package com.nowcoder.community;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(classes = CommunityApplication.class)
public class KafkaTests {

    // 发送消息需要bean
    @Autowired
    private KafkaProducer kafkaProducer;

    @Test
    public void testKafka() {
        kafkaProducer.sendMessage("test", "你好");
        kafkaProducer.sendMessage("test", "在吗");

        // 让这个方法阻塞一会儿，看消费者消费的过程
        try {
            Thread.sleep(1000 * 10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
// 希望bean由spring容器来管理
@Component
class KafkaProducer {

    // 生产者发送消息需要KafkaTemplate工具，这个工具被spring整合了在容器里面
    @Autowired
    private KafkaTemplate kafkaTemplate;

    // 发送消息的主题、传入的内容
    public void sendMessage(String topic, String content) {
        kafkaTemplate.send(topic, content);
    }

}
// 消费者的bean
@Component
class KafkaConsumer {

    //需要用到注解
    @KafkaListener(topics = {"test"})
    //这个方法是处理掉一个消息，调用方法时会对消息进行一个封装，从这个record就可以读到原始的消息
    public void handleMessage(ConsumerRecord record) {
        System.out.println(record.value());
    }


}
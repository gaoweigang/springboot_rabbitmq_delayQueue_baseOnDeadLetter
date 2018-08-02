package com.gwg.demo;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gwg.demo.mq.producer.UserProducer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 生产数据，然后启动springboot应用进行消费
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitMQTest {
	
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQTest.class);
	
	@Autowired
	private UserProducer userProducer;
	
	
	@Test
	public void produce(){
		userProducer.produce();
	}
	
	@Test
	public void produceTwo() throws IOException, TimeoutException{
		
		ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.13.64");
        factory.setUsername("hbsit");
        factory.setPassword("hbsit");
        factory.setPort(5672);
        factory.setVirtualHost("/hbsit");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        
        String message = "hello world!" + System.currentTimeMillis();
        // 设置延时属性
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        // 持久性 non-persistent (1) or persistent (2)
        AMQP.BasicProperties properties = builder.expiration("60000").deliveryMode(2).build();
        // routingKey =delay_queue 进行转发
        channel.basicPublish("EXCHANGE_DIRECT_TEST", "QUEUE_TEST", properties, message.getBytes());
        System.out.println("sent message: " + message + ",date:" + System.currentTimeMillis());
        // 关闭频道和连接
        channel.close();
        connection.close();
	}

	
}

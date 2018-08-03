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

	
}

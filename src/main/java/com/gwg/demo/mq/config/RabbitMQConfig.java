package com.gwg.demo.mq.config;

import com.gwg.demo.mq.common.*;
import com.gwg.demo.mq.message.process.impl.UserMessageProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * 
 * 生成ConnectionFactory
 *
 */
@Configuration
public class RabbitMQConfig {

	private static Logger logger = LoggerFactory.getLogger(RabbitMQConfig.class);

	// 测试 调试环境
	@Value("${rabbitmq.host}")
	private String host;
	@Value("${rabbitmq.username}")
	private String username;
	@Value("${rabbitmq.password}")
	private String password;
	@Value("${rabbitmq.port}")
	private Integer port;
	@Value("${rabbitmq.virtual-host}")
	private String virtualHost;//虚拟主机 

	//用户消息队列
	@Value("${rabbitmq.direct.exchange}")
	private String userExchangeName;
	@Value("${rabbitmq.queue}")
	private String userQueueName;
	@Value("${rabbitmq.routing}")
	private String userRouting;
	
	//死信队列
	@Value("${rabbitmq.deadLetter.direct.exchange}")
	private String deadLetterExchangeName;
	@Value("${rabbitmq.deadLetter.queue}")
	private String deadLetterQueueName;
	@Value("${rabbitmq.deadLetter.routing}")
	private String deadLetteruserRouting;
	

	@Bean
	public ConnectionFactory connectionFactory() {
		logger.info("connectionFactory, host:{}, port:{}, username:{}, password:{}", host, port, username, password);
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);

		connectionFactory.setUsername(username);
		connectionFactory.setPassword(password);
		connectionFactory.setVirtualHost(virtualHost);//设置虚拟主机
		// 设置消息手动确认模式
		connectionFactory.setPublisherConfirms(true); // enable confirm mode
		connectionFactory.setPublisherReturns(true);  // enable return mode
		// connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true);

		return connectionFactory;
	}

	/******************common *********************************************************************/
	@Bean
    public MQAccessBuilder mqAccessBuilder() throws IOException{
		MQAccessBuilder mqAccessBuilder = new MQAccessBuilder(connectionFactory());
		//构建正常队列，必须在死信队列之前
		mqAccessBuilder.buildQueue(userExchangeName, userRouting, userQueueName, connectionFactory(), "direct");
		//构建死信队列
		mqAccessBuilder.buildDeadLetterQueue(userExchangeName, deadLetterExchangeName, deadLetteruserRouting, deadLetterQueueName, connectionFactory(), "direct");
		return mqAccessBuilder;
	}

	/***************** messsage consumer ***************************************************/
    //采用线程池进行消费
	/*@Bean("userThreadPoolConsumer")
	public ThreadPoolConsumer userThreadPoolConsumer() throws IOException{
		ThreadPoolConsumer threadPoolConsumer = new ThreadPoolConsumer.ThreadPoolConsumerBuilder( null, null, deadLetterQueueName)
				.setMQAccessBuilder(mqAccessBuilder()).setMessageProcess(new UserMessageProcess())
				.setThreadCount(Constants.THREAD_COUNT).setIntervalMils(Constants.INTERVAL_MILS).build();
		threadPoolConsumer.start();//启动监听
		return threadPoolConsumer;
	}*/

	/***************** message producer*****************************************************/
	@Bean("userMessageProducer")
	public MessageProducer userMessageProducer() throws IOException {
		logger.info("messageSender, exchange:{}, queue:{} , routing:{}", userExchangeName, userQueueName, userRouting);
		return mqAccessBuilder().buildMessageSender(userExchangeName, userQueueName, userRouting);
	}

}

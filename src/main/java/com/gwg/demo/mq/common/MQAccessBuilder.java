package com.gwg.demo.mq.common;

import com.alibaba.fastjson.JSON;
import com.gwg.demo.mq.common.Constants;
import com.gwg.demo.mq.common.DetailRes;
import com.gwg.demo.mq.common.MessageWithTime;
import com.gwg.demo.mq.message.process.MessageProcess;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * 
 */
@Slf4j
public class MQAccessBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(MQAccessBuilder.class);
	
	private ConnectionFactory connectionFactory;

	public MQAccessBuilder(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public MessageProducer buildMessageSender(final String exchange, final String routingKey, final String queue)
			throws IOException {
		return buildMessageSender(exchange, routingKey, queue, "direct");
	}

	public MessageProducer buildTopicMessageSender(final String exchange, final String routingKey) throws IOException {
		return buildMessageSender(exchange, routingKey, null, "topic");
	}

	/***************************通过发送确定的方式确保数据发送broker**********************************************************************/
	// 1 构造template, exchange, routingkey等
	// 2 设置message序列化方法
	// 3 设置发送确认,确认消息有没有发送到broker代理服务器上
	// 4 构造sender方法
	public MessageProducer buildMessageSender(final String exchange, final String routingKey, final String queue,
                                              final String type) throws IOException {
		logger.info("buildMessageSender 创建新连接 start .....");
		Connection connection = connectionFactory.createConnection();
		//1.producer与consumer中队列构建抽离出来
	  /*if (type.equals("direct")) {
			logger.info("buildMessageSender 构造交换器和队列 ，并将交换器和队列绑定 start .....");
			buildQueue(exchange, routingKey, queue, connection, "direct");
		} else if (type.equals("topic")) {
			buildTopic(exchange, connection);
		}*/

		final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);

		rabbitTemplate.setMandatory(true);
		rabbitTemplate.setExchange(exchange);
		rabbitTemplate.setRoutingKey(routingKey);
		// 2
		logger.info("设置message序列化方法 start....");
		rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
		RetryCache retryCache = new RetryCache();

		//3.
		logger.info("设置发送确认,确认消息有没有路由到exchange,不管有没有路由到exchange上，都会回调该方法.....");
		rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
			logger.info("发送消息确认, correlationData:{}, ack:{},cause:{}", JSON.toJSON(correlationData), ack, cause);
			if (!ack) {
				logger.info("send message failed: " + cause + correlationData.toString());
			} else {
				logger.info("producer在收到确认消息后，删除本地缓存,correlationData:{}", JSON.toJSON(correlationData));
				//消息路由到exhange成功，删除本地缓存，我们发送的每条消息都会与correlationData相关，而correlationData中的id是我们自己指定的
				retryCache.del(Long.valueOf(correlationData.getId()));
			}
		});
		
		//3.1
		logger.info("确定消息有没有路由到queue,只有在消息从exchange路由到queue失败时候，才会调用该方法....");
		rabbitTemplate.setMandatory(true);
		rabbitTemplate.setReturnCallback((message, replyCode, replyText, tmpExchange, tmpRoutingKey) -> {
			logger.info("ReturnCallback start ....");
			try {
				Thread.sleep(Constants.ONE_SECOND);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			logger.info("send message failed: " + replyCode + " " + replyText);
			//消息路由到queue失败，重发
			rabbitTemplate.send(message);
		});

		// 4
		return new MessageProducer() {
			//初始化块
			{
				retryCache.setSender(this);
			}

			@Override
			public DetailRes produce(Object message) {
				long id = retryCache.generateId();
				long time = System.currentTimeMillis();

				return produce(new MessageWithTime(id, time, message));
			}

			@Override
			public DetailRes produce(MessageWithTime messageWithTime) {
				try {
					logger.info("在发送消息之前，先将消息进行本地缓存....");
					retryCache.add(messageWithTime);
					//将消息与CorrelationData关联，并发送
					rabbitTemplate.correlationConvertAndSend(messageWithTime.getMessage(),
							new CorrelationData(String.valueOf(messageWithTime.getId())));
				} catch (Exception e) {
					logger.error("将消息丢mq失败，异常：{}", e.getMessage());
					return new DetailRes(false, "");
				}

				return new DetailRes(true, "");
			}
		};
	}

	/*************************** Consumer采用消费失败消息重新投递*********************************************/
	public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey, final String queue,
                                                    final MessageProcess<T> messageProcess) throws IOException {
		return buildMessageConsumer(exchange, routingKey, queue, messageProcess, "direct");
	}

	public <T> MessageConsumer buildTopicMessageConsumer(String exchange, String routingKey, final String queue,
                                                         final MessageProcess<T> messageProcess) throws IOException {
		return buildMessageConsumer(exchange, routingKey, queue, messageProcess, "topic");
	}

	// 1 创建连接和channel
	// 2 设置message序列化方法
	// 3 consume
	public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey, final String queue,
                                                    final MessageProcess<T> messageProcess, String type) throws IOException {
		final Connection connection = connectionFactory.createConnection();

		//1.producer与consumer中队列构建抽离出来
		//buildQueue(exchange, routingKey, queue, connection, type);

		// 2
		final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();
		final MessageConverter messageConverter = new Jackson2JsonMessageConverter();

		// 3 匿名类
		return new MessageConsumer() {
			Channel channel = connection.createChannel(false);

			// 1 通过basicGet获取原始数据
			// 2 将原始数据转换为特定类型的包
			// 3 处理数据
			// 4 手动发送ack确认
			@Override
			public DetailRes consume() {
				try {
					// 1
					logger.info("通过basicGet获取原始数据 start........，队列：{}", queue);
					GetResponse response = channel.basicGet(queue, false);
					while (response == null) {
						logger.info("如果没有获取到原始数据，则睡眠一秒之后再尝试获取，具体休眠时间根据实际情况设置, 队列：{}", queue);
						response = channel.basicGet(queue, false);
						Thread.sleep(Constants.TEN_SECOND);
					}
                    logger.info("原始数据： props:{}, MessageCount:{}, Envelope：{}", response.getProps(), response.getMessageCount(), response.getEnvelope());
					Message message = new Message(response.getBody(), messagePropertiesConverter
							.toMessageProperties(response.getProps(), response.getEnvelope(), "UTF-8"));

					// 2
					@SuppressWarnings("unchecked")
					T messageBean = (T) messageConverter.fromMessage(message);

					// 3
					DetailRes detailRes;

					try {
						logger.info("process 开始处理消息 start......,消息内容：{}", JSON.toJSON(messageBean));
						detailRes = messageProcess.process(messageBean);
					} catch (Exception e) {
						log.error("exception", e);
						detailRes = new DetailRes(false, "process exception: " + e);
					}

					// 4 只有在消息处理成功后发送ack确认，或失败后发送nack使信息重新投递，使用springboot来改写测试信息重新投递
					if (detailRes.isSuccess()) {
						logger.info("ack确认：{}", response.getEnvelope().getDeliveryTag());
						/**
					     * 确认收到的一个或几个消息。由包含接收消息确认的com.rabbitmq.client.AMQP.Basic.GetOk
					     * 或com.rabbitmq.client.AMQP.Basic.Deliver方法提供deliveryTag。
					     * 参数：deliveryTag 来自com.rabbitmq.client.AMQP.Basic.GetOk或com.rabbitmq.client.AMQP.Basic.Deliver的标签
					     * 参数：multiple=true 确认所有信息，包括提供的传送标签;
					     *       multiple=false  只确认所提供的传送标签。
					     */
						channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
					} else {
						// 避免过多失败log
						Thread.sleep(Constants.ONE_SECOND);
						log.info("消费死信队列中的消息，消费失败，消息丢弃:{} " ,detailRes.getErrMsg());
						channel.basicNack(response.getEnvelope().getDeliveryTag(), false, false);
					}

					return detailRes;
				} catch (InterruptedException e) {
					log.error("exception", e);
					return new DetailRes(false, "interrupted exception " + e.toString());
				} catch (ShutdownSignalException | ConsumerCancelledException | IOException e) {
					log.error("exception", e);

					try {
						channel.close();
					} catch (IOException | TimeoutException ex) {
						log.error("exception", ex);
					}

					channel = connection.createChannel(false);

					return new DetailRes(false, "shutdown or cancelled exception " + e.toString());
				} catch (Exception e) {
					e.printStackTrace();
					log.info("exception : ", e);

					try {
						channel.close();
					} catch (IOException | TimeoutException ex) {
						ex.printStackTrace();
					}

					channel = connection.createChannel(false);

					return new DetailRes(false, "exception " + e.toString());
				}
			}
		};
	}
    
	/**
	 * 构建交换器，队列，并将交换器和队列绑定
     */
	public void buildQueue(String exchange, String routingKey, final String queue, ConnectionFactory connectionFactory, String type, String deadLetterExchange, String deadLetterrRoutingKey)
			throws IOException {
		logger.info("buildQueue, exchange:{}, routingKey:{}, queue:{}, connectionFactory:{}, type:{}",exchange ,routingKey, queue, connectionFactory, type);
		logger.info("createConnection ..........");
		Connection connection = connectionFactory.createConnection();
		
		//创建一个新的通道，使用内部分配的通道号。 transactional true :通道是否应该支持事物
		logger.info("createChannel .......");
		Channel channel = connection.createChannel(false);

		if (type.equals("direct")) {
			/*
			 * 1.交换器名称
			 * 2.交换器类型 
			 * 3.durable = true : 声明一个持久的交换器(交换器在服务器重新启动后仍然有效)
			 * 4.autoDelete = true :当交换器不再使用的的时候 服务器应该删除交换器 
			 * 5.参数 ： 交换器的其他属性(构造参数)
			 */
			logger.info("direct exchangeDeclare ..........");
			channel.exchangeDeclare(exchange, "direct", true, false, null);
		} else if (type.equals("topic")) {
			logger.info("topic exchangeDeclare ..........");
			channel.exchangeDeclare(exchange, "topic", true, false, null);
		}
		logger.info("queueDeclare ..........");
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-dead-letter-exchange", deadLetterExchange);//指定队列使用的死信交换器
		args.put("x-message-ttl", 60000);//设置消息在队列中的存活时间,消息过期后丢入死信队列， 时间单位毫秒
		//你也可以为这个DLX指定routing key，如果没有特殊指定，则使用原队列的routing key
		args.put("x-dead-letter-routing-key", deadLetterrRoutingKey);//与死信队列的routingKey相同
		channel.queueDeclare(queue, true, false, false, args);//args参数指定死信队列，如果消费失败，消息会丢入死信队列
		
		logger.info("queueBind ..........");
		/*
		 * 在这里Routingkey的作用：
		 * 1.exchange会根据queue对那个routingKey感兴趣，将消息路由到相应的queue里面
		 * 2.同一个queue可以通过多个routingKey感兴趣
		 */
		channel.queueBind(queue, exchange, routingKey);        

		try {
			channel.close();
		} catch (TimeoutException e) {
			log.info("close channel time out ", e);
		}
	}

	private void buildTopic(String exchange, Connection connection) throws IOException {
		Channel channel = connection.createChannel(false);
		channel.exchangeDeclare(exchange, "topic", true, false, null);
	}

	// for test
	public int getMessageCount(final String queue) throws IOException {
		Connection connection = connectionFactory.createConnection();
		final Channel channel = connection.createChannel(false);
		final AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queue);

		return declareOk.getMessageCount();
	}
	
	/****************Dead Letter  死信处理   ***********************************************************/
	public void buildDeadLetterQueue(String deadLetterExchange, String deadLetterrRoutingKey, final String deadLetterQueue, ConnectionFactory connectionFactory, String type) throws IOException{
		logger.info("构建死信队列 start ......");
		logger.info("DeadLetter buildDeadLetterQueue,deadLetterExchange:{}, deadLetterRoutingKey:{}, deadLetterQueue:{}, connectionFactory:{}, type:{}",deadLetterExchange, deadLetterrRoutingKey, deadLetterQueue, connectionFactory, type);
		Connection connection = connectionFactory.createConnection();
		Channel channel = connection.createChannel(false);
		if (type.equals("direct")) {
			/*
			 * 1.交换器名称
			 * 2.交换器类型 
			 * 3.durable = true : 声明一个持久的交换器(交换器在服务器重新启动后仍然有效)
			 * 4.autoDelete = true :当交换器不再使用的的时候 服务器应该删除交换器 
			 * 5.参数 ： 交换器的其他属性(构造参数)
			 */
			logger.info("direct exchangeDeclare ..........");
			channel.exchangeDeclare(deadLetterExchange, "direct", true, false, null);//死信交换器
		} else if (type.equals("topic")) {
			logger.info("topic exchangeDeclare ..........");
			channel.exchangeDeclare(deadLetterExchange, "topic");
		}
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-message-ttl", 3600000);//设置消息在死信队列中的存活时间为1小时
		channel.queueDeclare(deadLetterQueue, false, false, false, args);
		//注意第二参数不是死信交换器，在这里死信队列deadLetterQueue对deadLetterrRoutingKey感兴趣
		channel.queueBind(deadLetterQueue, deadLetterExchange, deadLetterrRoutingKey);
		try {
			channel.close();
		} catch (TimeoutException e) {
			log.info("close channel time out ", e);
		}
	}
}

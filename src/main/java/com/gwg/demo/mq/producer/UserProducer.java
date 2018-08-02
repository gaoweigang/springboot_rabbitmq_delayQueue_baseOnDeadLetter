package com.gwg.demo.mq.producer;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gwg.demo.mq.common.MessageProducer;
import com.gwg.demo.mq.message.UserMessage;

@Component
public class UserProducer {

	private static final Logger logger = LoggerFactory
			.getLogger(UserProducer.class);

	@Autowired
	private MessageProducer<UserMessage> messageProducer;
	
	public void produce() {
		logger.info("消息 生产 start .....");
		for(int i = 0; i < 50; i++){
			messageProducer.produce(new UserMessage(1000+i, "gaoweiang"+i, new Date()));		
		}

	}

}

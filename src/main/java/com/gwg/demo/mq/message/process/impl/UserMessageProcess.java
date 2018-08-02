package com.gwg.demo.mq.message.process.impl;



import com.gwg.demo.mq.common.DetailRes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.gwg.demo.mq.message.UserMessage;
import com.gwg.demo.mq.message.process.MessageProcess;

public class UserMessageProcess<T> implements MessageProcess<T>{

	private static final Logger logger = LoggerFactory.getLogger(UserMessageProcess.class);
	
	@Override
	public DetailRes process(T message) {
		logger.info("process 消息处理：{}", JSON.toJSON(message));
		return new DetailRes(false, null);//消费失败返回
		//return new DetailResult(true, null);//消费成功返回

	}
}


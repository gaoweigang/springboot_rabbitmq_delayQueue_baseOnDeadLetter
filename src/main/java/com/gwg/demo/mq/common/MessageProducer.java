package com.gwg.demo.mq.common;

public interface MessageProducer<T> {
	
	public DetailRes produce(T message);

	public DetailRes produce(MessageWithTime messageWithTime);

}

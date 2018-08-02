package com.gwg.demo.mq.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gwg.demo.mq.message.process.MessageProcess;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 在对消息处理的过程中，我们期望多线程并行执行来增加效率，因此对consumer做了一个线程池的封装。
 */
@Data
public class ThreadPoolConsumer<T> {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolConsumer.class);

    private ExecutorService executor;
    private volatile boolean stop = false;
    private final ThreadPoolConsumerBuilder<T> infoHolder;

    private ThreadPoolConsumer(ThreadPoolConsumerBuilder<T> threadPoolConsumerBuilder) {
        this.infoHolder = threadPoolConsumerBuilder;
        executor = Executors.newFixedThreadPool(threadPoolConsumerBuilder.threadCount);
    }

    //1 构造messageConsumer
    //2 执行consume
    public void start() throws IOException {
        for (int i = 0; i < infoHolder.threadCount; i++) {
            //1
            final MessageConsumer messageConsumer = infoHolder.mqAccessBuilder.buildMessageConsumer(infoHolder.exchange,
                    infoHolder.routingKey, infoHolder.queue, infoHolder.messageProcess, infoHolder.type);

            executor.execute(new Runnable() {
            	/**
            	 * 启动特定的线程数不断的从broker拉数据
            	 */
                @Override
                public void run() {
                    while (!stop) {//循环拉取数据
                        try {
                            //2
                            DetailRes detailRes = messageConsumer.consume();

                            if (infoHolder.intervalMils > 0) {
                                try {//处理时间间隔
                                    Thread.sleep(infoHolder.intervalMils);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                    logger.info("interrupt ", e);
                                }
                            }

                            if (!detailRes.isSuccess()) {
                                logger.info("run error " + detailRes.getErrMsg());
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.info("run exception ", e);
                        }
                    }
                }
            });
        }
        
        /*
         * 这个方法的意思就是在jvm中增加一个关闭的钩子，当jvm关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook
         * 添加的钩子，当系统执行完这些钩子后，jvm才会关闭。所以这些钩子可以在jvm关闭的时候进行内存清理、对象销毁等操作。
         */
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    public void stop() {
        this.stop = true;

        try {
            Thread.sleep(Constants.ONE_SECOND);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    //构造器
    public static class ThreadPoolConsumerBuilder<T> {
        private int threadCount;
        private long intervalMils;
        private MQAccessBuilder mqAccessBuilder;
        private String exchange;
        private String routingKey;
        private String queue;
        private String type = "direct";
        private MessageProcess<T> messageProcess;

        public ThreadPoolConsumerBuilder(String exchange, String routingKey, String queue){
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.queue = queue;
        }

        public ThreadPoolConsumerBuilder<T> setThreadCount(int threadCount) {
            this.threadCount = threadCount;

            return this;
        }

        public ThreadPoolConsumerBuilder<T> setIntervalMils(long intervalMils) {
            this.intervalMils = intervalMils;

            return this;
        }

        public ThreadPoolConsumerBuilder<T> setMQAccessBuilder(MQAccessBuilder mqAccessBuilder) {
            this.mqAccessBuilder = mqAccessBuilder;

            return this;
        }

        public ThreadPoolConsumerBuilder<T> setExchange(String exchange) {
            this.exchange = exchange;

            return this;
        }

        public ThreadPoolConsumerBuilder<T> setRoutingKey(String routingKey) {
            this.routingKey = routingKey;

            return this;
        }

        public ThreadPoolConsumerBuilder<T> setQueue(String queue) {
            this.queue = queue;

            return this;
        }

        public ThreadPoolConsumerBuilder<T> setType(String type) {
            this.type = type;

            return this;
        }

        public ThreadPoolConsumerBuilder<T> setMessageProcess(MessageProcess<T> messageProcess) {
            this.messageProcess = messageProcess;

            return this;
        }

        public ThreadPoolConsumer<T> build() {
            return new ThreadPoolConsumer<T>(this);
        }
    }
}

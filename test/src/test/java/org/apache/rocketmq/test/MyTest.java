package org.apache.rocketmq.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.omg.SendingContext.RunTime;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author liulin
 * @version $Id: MyTest.java, v0.1 2020/8/7 17:56 liulin Exp $$
 */
public class MyTest {
    private static final String DEFAULT_CHARSET = "UTF-8";
    private static final String TOPIC = "HelloTopic";

    public static void main(String[] args) {
//        SendMessageProcessor p = new SendMessageProcessor();
//        System.out.println(p  instanceof  AsyncNettyRequestProcessor);
////        AsyncNettyRequestProcessor
    }

    @Test
    //同步发送
    public void syncProduce() throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setProducerGroup("test_producer_sync");
        producer.start();
        Message message = new Message(TOPIC, "TagB", "key01", ("Hello "+System.currentTimeMillis()).getBytes(DEFAULT_CHARSET));
        SendResult result = producer.send(message);
        System.out.printf("SendResult: %s %n", result);
        producer.shutdown();
    }

    @Test
    //异步发送
    public void asyncProduce() throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_sync");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        Message message = new Message(TOPIC, "TagA", "key01", ("Hello "+System.currentTimeMillis()).getBytes(DEFAULT_CHARSET));
        //延时消息
        message.setDelayTimeLevel(3);
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("SendResult: %s %n", sendResult);
            }

            @Override
            public void onException(Throwable e) {
                System.out.printf("Send error: %s %n", e.getMessage());
            }
        });
        Thread.sleep(2000);
        producer.shutdown();
    }

    @Test
    //事务消息
    public void transactionProduce() throws UnsupportedEncodingException, MQClientException, InterruptedException {
        TransactionMQProducer producer = new TransactionMQProducer("test_producer_sync");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
//                throw new RuntimeException("");
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                System.out.println("checkLocalTransaction");
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();
        Message message = new Message(TOPIC, "TagA", "key01", ("Hello "+System.currentTimeMillis()).getBytes(DEFAULT_CHARSET));
        TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, null);
        System.out.printf("SendResult: %s %n", transactionSendResult);
        Thread.sleep(20000);
//        Thread.currentThread().join();
        producer.shutdown();
    }
    @Test
    public void pushConsumerTest() throws MQClientException, InterruptedException {
        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("test_consumer");
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
        pushConsumer.subscribe(TOPIC,"TagB");
        pushConsumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.getMessageQueue().getQueueId();
                System.out.printf("%s received message: %s with tag %s from queue %d %n",Thread.currentThread().getName(),msgs.stream().map(o-> {
                    try {
                        return new String(o.getBody(),DEFAULT_CHARSET);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    return "";
                }).collect(Collectors.joining(",")),
                        msgs.stream().map(Message::getTags).collect(Collectors.joining(",")),
                        context.getMessageQueue().getQueueId());
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        pushConsumer.start();
//        Thread.sleep(5000);
        Thread.currentThread().join();
        pushConsumer.shutdown();
    }
}

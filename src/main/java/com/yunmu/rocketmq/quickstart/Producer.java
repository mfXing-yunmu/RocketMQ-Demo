package com.yunmu.rocketmq.quickstart;

import com.yunmu.rocketmq.common.Const;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * @Author: mfXing
 * @CreateDate: 2020-01-10 15:49
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("test_quick_producer_name");

        producer.setNamesrvAddr(Const.NAMESRV_ADDR_SINGLE);

        producer.setSendMsgTimeout(20000);

        producer.start();

        for(int i = 0;i < 1;i++){
            // 1、创建消息
            Message message = new Message("test_quick_topic",
                    "Tag",
                    "key",
                    ("Hello World RocketMQ").getBytes());

            // 2.1、	同步发送消息
//			if(i == 1) {
//				message.setDelayTimeLevel(3);
//			}

            SendResult sr = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer queueNumber = (Integer)arg;
                    return mqs.get(queueNumber);
                }
            }, 2);
            System.err.println(sr);

//            SendResult sendResult = producer.send(message);
//            SendStatus status = sr.getSendStatus();
//            System.out.println("发出消息 ：" + sendResult);

            //  2.2 异步发送消息
			producer.send(message, new SendCallback() {
				//rabbitmq急速入门的实战: 可靠性消息投递
				@Override
				public void onSuccess(SendResult sendResult) {
					System.err.println("msgId: " + sendResult.getMsgId() + ", status: " + sendResult.getSendStatus());
				}
				@Override
				public void onException(Throwable e) {
					e.printStackTrace();
					System.err.println("------发送失败");
				}
			});
        }

        producer.shutdown();
    }
}

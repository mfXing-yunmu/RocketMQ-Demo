package com.yunmu.rocketmq.quickstart;

import com.yunmu.rocketmq.common.Const;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @Author: mfXing
 * @CreateDate: 2020-01-10 15:49
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("test_quick_producer_name");

        producer.setNamesrvAddr(Const.NAMESRV_ADDR);

        producer.setSendMsgTimeout(20000);

        producer.start();

        for(int i = 0;i < 5;i++){
            // 1、创建消息
            Message message = new Message("test_quick_topic",
                    "Tag",
                    "key",
                    ("Hello World RocketMQ").getBytes());

            // 2、发送消息
            SendResult sendResult = producer.send(message);

            System.out.println("发出消息 ：" + sendResult);
        }

        producer.shutdown();
    }
}

package com.yunmu.rocketmq.quickstart;

import com.yunmu.rocketmq.common.Const;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @Author: mfXing
 * @CreateDate: 2020-01-10 15:50
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_quick_consumer_name");

        consumer.setNamesrvAddr(Const.NAMESRV_ADDR);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        consumer.subscribe("test_quick_topic","*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt messageExt = msgs.get(0);

                try {
                    String topic = messageExt.getTopic();

                    String tags = messageExt.getTags();

                    String keys = messageExt.getKeys();

                    if("key1".equals(keys)){
                        System.err.println("消息消费失败。。");
                        int a = 1/0;
                    }

                    String mshBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);

                    System.out.println("topic : " + topic + ", tags : " + tags + ", keys : " + keys + ", msgBody : " + mshBody);
                }catch (Exception e){
                    e.printStackTrace();
                    int reconsumeTimes = messageExt.getReconsumeTimes();
                    System.out.println("reconsumeTimes : " + reconsumeTimes);

                    if(reconsumeTimes == 3){
                        // 记录日志
                        // 做补偿机制
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    return  ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("consumer start!");
    }
}

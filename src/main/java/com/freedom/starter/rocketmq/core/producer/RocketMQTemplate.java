package com.freedom.starter.rocketmq.core.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import com.alibaba.rocketmq.common.message.Message;
import com.freedom.starter.rocketmq.message.RocketMQHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.MessagingException;
import org.springframework.util.Assert;

import java.nio.charset.Charset;

/**
 * 发送RocketMQ消息的模板类
 */
public class RocketMQTemplate /*extends AbstractMessageSendingTemplate<String>*/ implements InitializingBean, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQTemplate.class);

    /** 生产者，自动配置RocketMQTemplate时设置 */
    private DefaultMQProducer producer;

    /** 消息默认字符集 */
    private String charset = "UTF-8";

    /** 默认队列选择器 */
    private MessageQueueSelector defalutMessageQueueSelector = new SelectMessageQueueByHash();


    public String getCharset() {
        return charset;
    }
    public void setCharset(String charset) {
        this.charset = charset;
    }
    public DefaultMQProducer getProducer() {
        return producer;
    }
    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    /**
     * 实现InitializingBean接口的方法
     * 在所有属性设置完成后，由BeanFactory调用此方法
     * 用于启动DefaultMQProducer
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(producer, "DefaultMQProducer can not null");
        producer.start();
    }

    /**
     * 实现DisposableBean接口的方法
     * 在BeanFactory销毁Bean之前释放资源
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        if(producer != null){
            producer.shutdown();
        }
    }


    /**
     * 实现AbstractMessageSendingTemplate的方法
     * 发送消息
     * @param destination  目的地，即topic
     * @param message  spring message实例（payload-消息体，headers-消息头）
     */
    //@Override
    //protected void doSend(String destination, Message message) {
    //
    //}


    /**
     * 同步发送消息
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @return
     */
    public SendResult send(String topic, String tag, Object message) {
        return send(topic, tag, message, producer.getSendMsgTimeout());
    }

    /**
     * 同步发送消息
     * @param topic      主题
     * @param tag        标签
     * @param message    消息体，Object类型
     * @param timeout    消息发送超时时间，单位毫秒
     * @return
     */
    public SendResult send(String topic, String tag, Object message, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, null);
        return send(topic, tag, rocketmqMsg, timeout);
    }

    /**
     * 同步发送消息
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param header   消息头
     * @return
     */
    public SendResult send(String topic, String tag, Object message, RocketMQHeader header) {
        return send(topic, tag, message, header, producer.getSendMsgTimeout());
    }

    /**
     * 同步发送消息
     * @param topic     主题
     * @param tag       标签
     * @param message   消息体，Object类型
     * @param header    消息头
     * @param timeout   消息发送超时时间，单位毫秒
     * @return
     */
    public SendResult send(String topic, String tag, Object message, RocketMQHeader header, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, header);
        return send(topic, tag, rocketmqMsg, timeout);
    }

    /**
     * 真正调用同步发送消息
     * @param topic          主题
     * @param tag            标签
     * @param rocketmqMsg   rocketmq message类型
     * @param timeout        消息发送超时时间，单位毫秒
     * @return
     */
    private SendResult send(String topic, String tag, Message rocketmqMsg, long timeout) {
        if(topic==null || "".equals(topic)){
            throw new IllegalArgumentException("'topic' cannot be null");
        }
        if(rocketmqMsg==null || rocketmqMsg.getBody()==null || rocketmqMsg.getBody().length<=0){
            throw new IllegalArgumentException("'message' and 'message.body' cannot be null");
        }

        try {
            long now = System.currentTimeMillis();
            SendResult sendResult = producer.send(rocketmqMsg, timeout);
            long costTime = System.currentTimeMillis() - now;
            logger.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        }
        catch (Exception e) {
            logger.error("syncSend failed. topic:{}, tag:{}, messageBody:{} ", topic, tag, rocketmqMsg.getBody());
            throw new MessagingException(e.getMessage(), e);
        }
    }





    /**
     * 同步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param selectQueueKey  用于选择队列的key，使用SelectMessageQueueByHash
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, String selectQueueKey) {
        return sendOrderly(topic, tag, message, selectQueueKey, producer.getSendMsgTimeout());
    }

    /**
     * 同步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param selectQueueKey    用于选择队列的key，使用SelectMessageQueueByHash
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, String selectQueueKey, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, null);
        return sendOrderly(topic, tag, rocketmqMsg, null, selectQueueKey, timeout);
    }

    /**
     * 同步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param selectQueueKey  用于选择队列的key，使用SelectMessageQueueByHash
     * @param header   消息头
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, String selectQueueKey, RocketMQHeader header) {
        return sendOrderly(topic, tag, message, selectQueueKey, header, producer.getSendMsgTimeout());
    }

    /**
     * 同步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param selectQueueKey    用于选择队列的key，使用SelectMessageQueueByHash
     * @param header             消息头
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, String selectQueueKey, RocketMQHeader header, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, header);
        return sendOrderly(topic, tag, rocketmqMsg, null, selectQueueKey, timeout);
    }

    /**
     * 同步发送顺序消息，自定义队列选择器
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey  用于选择队列的key
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey) {
        return sendOrderly(topic, tag, message, customMessageQueueSelector, selectQueueKey, producer.getSendMsgTimeout());
    }

    /**
     * 同步发送顺序消息
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey    用于选择队列的key
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, null);
        return sendOrderly(topic, tag, rocketmqMsg, customMessageQueueSelector, selectQueueKey, timeout);
    }

    /**
     * 同步发送顺序消息，自定义队列选择器
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey  用于选择队列的key
     * @param header           消息头
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey, RocketMQHeader header) {
        return sendOrderly(topic, tag, message, customMessageQueueSelector, selectQueueKey, header, producer.getSendMsgTimeout());
    }

    /**
     * 同步发送顺序消息
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey    用于选择队列的key
     * @param header             消息头
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public SendResult sendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey, RocketMQHeader header, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, header);
        return sendOrderly(topic, tag, rocketmqMsg, customMessageQueueSelector, selectQueueKey, timeout);
    }

    /**
     * 真正调用同步发送顺序消息
     * @param topic                主题
     * @param tag                  标签
     * @param rocketmqMsg         rocketmq message
     * @param messageQueueSelector  队列选择器，如果没传默认使用SelectMessageQueueByHash
     * @param selectQueueKey     用于选择队列的key，使用SelectMessageQueueByHash
     * @param timeout             消息发送超时时间，单位毫秒
     * @return
     */
    private SendResult sendOrderly(String topic, String tag, Message rocketmqMsg, MessageQueueSelector messageQueueSelector, String selectQueueKey, long timeout) {
        if(topic==null || "".equals(topic)){
            throw new IllegalArgumentException("'topic' cannot be null");
        }
        if(rocketmqMsg==null || rocketmqMsg.getBody()==null || rocketmqMsg.getBody().length<=0){
            throw new IllegalArgumentException("'message' and 'message.body' cannot be null");
        }
        if(selectQueueKey==null || "".equals(selectQueueKey)){
            throw new IllegalArgumentException("'selectQueueKey' cannot be null");
        }

        if(messageQueueSelector == null){
            messageQueueSelector = this.defalutMessageQueueSelector;
        }

        try {
            long now = System.currentTimeMillis();
            SendResult sendResult = producer.send(rocketmqMsg, messageQueueSelector, selectQueueKey, timeout);
            long costTime = System.currentTimeMillis() - now;
            logger.debug("send message cost: {} ms, msgId:{}", costTime, sendResult.getMsgId());
            return sendResult;
        }
        catch (Exception e) {
            logger.error("syncSendOrderly failed. topic:{}, tag:{}, messageBody:{} ", topic, tag, rocketmqMsg.getBody());
            throw new MessagingException(e.getMessage(), e);
        }
    }




    /**
     * 发送异步消息
     * @param topic          主题
     * @param tag            标签
     * @param message        消息体，Object类型
     * @param sendCallback   发送结束后的回调方法
     */
    public void asyncSend(String topic, String tag, Object message, SendCallback sendCallback) {
        asyncSend(topic, tag, message, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * 发送异步消息
     * @param topic          主题
     * @param tag            标签
     * @param message        消息体，Object类型
     * @param sendCallback   发送结束后的回调方法
     * @param timeout         消息发送超时时间，单位毫秒
     */
    public void asyncSend(String topic, String tag, Object message, SendCallback sendCallback, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, null);
        asyncSend(topic, tag, rocketmqMsg, sendCallback, timeout);
    }

    /**
     * 发送异步消息
     * @param topic          主题
     * @param tag            标签
     * @param message        消息体，Object类型
     * @param header         消息头
     * @param sendCallback   发送结束后的回调方法
     */
    public void asyncSend(String topic, String tag, Object message, RocketMQHeader header, SendCallback sendCallback) {
        asyncSend(topic, tag, message, header, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * 发送异步消息
     * @param topic          主题
     * @param tag            标签
     * @param message        消息体，Object类型
     * @param header         消息头
     * @param sendCallback   发送结束后的回调方法
     * @param timeout         消息发送超时时间，单位毫秒
     */
    public void asyncSend(String topic, String tag, Object message, RocketMQHeader header, SendCallback sendCallback, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, header);
        asyncSend(topic, tag, rocketmqMsg, sendCallback, timeout);
    }

    /**
     * 真正的发送异步消息，用于对响应时间敏感的业务场景，方法会立刻返回，在发送结束后会执行SendCallback
     * @param topic           主题
     * @param tag              标签
     * @param rocketmqMsg     rocketmq message
     * @param sendCallback    发送结束后的回调方法
     * @param timeout          消息发送超时时间，单位毫秒
     */
    private void asyncSend(String topic, String tag, Message rocketmqMsg, SendCallback sendCallback, long timeout) {
        if(topic==null || "".equals(topic)){
            throw new IllegalArgumentException("'topic' cannot be null");
        }
        if(rocketmqMsg==null || rocketmqMsg.getBody()==null || rocketmqMsg.getBody().length<=0){
            throw new IllegalArgumentException("'message' and 'message.body' cannot be null");
        }
        if(sendCallback==null){
            throw new IllegalArgumentException("'sendCallback' cannot be null");
        }

        try {
            producer.send(rocketmqMsg, sendCallback, timeout);
        }
        catch (Exception e) {
            logger.error("asyncSend failed. topic:{}, tag:{}, messageBody:{} ", topic, tag, rocketmqMsg.getBody());
            throw new MessagingException(e.getMessage(), e);
        }
    }





    /**
     * 异步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param selectQueueKey  用于选择队列的key，使用SelectMessageQueueByHash
     * @param sendCallback     发送完后的回调方法
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, String selectQueueKey, SendCallback sendCallback) {
        asyncSendOrderly(topic, tag, message, selectQueueKey, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * 异步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param selectQueueKey    用于选择队列的key，使用SelectMessageQueueByHash
     * @param sendCallback       发送完后的回调方法
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, String selectQueueKey, SendCallback sendCallback, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, null);
        asyncSendOrderly(topic, tag, rocketmqMsg, null, selectQueueKey, sendCallback, timeout);
    }

    /**
     * 异步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param selectQueueKey  用于选择队列的key，使用SelectMessageQueueByHash
     * @param header   消息头
     * @param sendCallback     发送完后的回调方法
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, String selectQueueKey, RocketMQHeader header, SendCallback sendCallback) {
        asyncSendOrderly(topic, tag, message, selectQueueKey, header, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * 异步发送顺序消息，使用默认的MessageQueueSelector
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param selectQueueKey    用于选择队列的key，使用SelectMessageQueueByHash
     * @param header             消息头
     * @param sendCallback       发送完后的回调方法
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, String selectQueueKey, RocketMQHeader header, SendCallback sendCallback, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, header);
        asyncSendOrderly(topic, tag, rocketmqMsg, null, selectQueueKey, sendCallback, timeout);
    }

    /**
     * 异步发送顺序消息，自定义队列选择器
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey  用于选择队列的key
     * @param sendCallback     发送完后的回调方法
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey, SendCallback sendCallback) {
        asyncSendOrderly(topic, tag, message, customMessageQueueSelector, selectQueueKey, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * 异步发送顺序消息
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey    用于选择队列的key
     * @param sendCallback       发送完后的回调方法
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey, SendCallback sendCallback, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, null);
        asyncSendOrderly(topic, tag, rocketmqMsg, customMessageQueueSelector, selectQueueKey, sendCallback, timeout);
    }

    /**
     * 异步发送顺序消息，自定义队列选择器
     * @param topic    主题
     * @param tag       标签
     * @param message  消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey  用于选择队列的key
     * @param header           消息头
     * @param sendCallback     发送完后的回调方法
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey, RocketMQHeader header, SendCallback sendCallback) {
        asyncSendOrderly(topic, tag, message, customMessageQueueSelector, selectQueueKey, header, sendCallback, producer.getSendMsgTimeout());
    }

    /**
     * 异步发送顺序消息
     * @param topic              主题
     * @param tag                 标签
     * @param message            消息体，Object类型
     * @param customMessageQueueSelector  自定义队列选择器
     * @param selectQueueKey    用于选择队列的key
     * @param header             消息头
     * @param sendCallback       发送完后的回调方法
     * @param timeout            消息发送超时时间，单位毫秒
     * @return
     */
    public void asyncSendOrderly(String topic, String tag, Object message, MessageQueueSelector customMessageQueueSelector, String selectQueueKey, RocketMQHeader header, SendCallback sendCallback, long timeout) {
        Message rocketmqMsg = doConvert(topic, tag, message, header);
        asyncSendOrderly(topic, tag, rocketmqMsg, customMessageQueueSelector, selectQueueKey, sendCallback, timeout);
    }

    /**
     * 真正调用异步发送顺序消息
     * @param topic                主题
     * @param tag                  标签
     * @param rocketmqMsg         rocketmq message
     * @param messageQueueSelector  队列选择器，如果没传默认使用SelectMessageQueueByHash
     * @param selectQueueKey     用于选择队列的key，使用SelectMessageQueueByHash
     * @param sendCallback        发送完成后的回调方法
     * @param timeout             消息发送超时时间，单位毫秒
     * @return
     */
    private void asyncSendOrderly(String topic, String tag, Message rocketmqMsg, MessageQueueSelector messageQueueSelector, String selectQueueKey, SendCallback sendCallback, long timeout) {
        if(topic==null || "".equals(topic)){
            throw new IllegalArgumentException("'topic' cannot be null");
        }
        if(rocketmqMsg==null || rocketmqMsg.getBody()==null || rocketmqMsg.getBody().length<=0){
            throw new IllegalArgumentException("'message' and 'message.body' cannot be null");
        }
        if(selectQueueKey==null || "".equals(selectQueueKey)){
            throw new IllegalArgumentException("'selectQueueKey' cannot be null");
        }
        if(sendCallback==null){
            throw new IllegalArgumentException("'sendCallback' cannot be null");
        }

        if(messageQueueSelector == null){
            messageQueueSelector = this.defalutMessageQueueSelector;
        }

        try {
            producer.send(rocketmqMsg, messageQueueSelector, selectQueueKey, sendCallback, timeout);
        }
        catch (Exception e) {
            logger.error("asyncSendOrderly failed. topic:{}, tag:{}, messageBody:{} ", topic, tag, rocketmqMsg.getBody());
            throw new MessagingException(e.getMessage(), e);
        }
    }







    /**
     *  消息转换为RocketMQ Message
     * @param topic    主题
     * @param tag      标签
     * @param message  消息体
     * @param header   消息头
     * @return
     */
    protected Message doConvert(String topic, String tag, Object message, RocketMQHeader header) {
        String content;

        if (message instanceof String) {
            content = (String) message;
        }
        //如果消息不是字符串类型，默认使用fastjson
        else {
            content = JSON.toJSONString(message);
        }

        byte[] messageBody = content.getBytes(Charset.forName(charset));
        Message rocketmqMsg = new Message(topic, tag, messageBody);

        //如果RocketMQHeader不为空
        if(header != null){
            //设置业务keys
            String keys = header.getKeys();
            if(keys!=null && !"".equals(keys)){
                rocketmqMsg.setKeys(keys);
            }

            //设置waitStoreMsgOK，默认值true
            boolean waitStoreMsgOK = header.isWaitStoreMsgOK();
            rocketmqMsg.setWaitStoreMsgOK(waitStoreMsgOK);
        }

        return rocketmqMsg;
    }


    /**
     * 单向传输消息，不需要等待broker的消息确认
     * @param topic     主题
     * @param tag        标签
     * @param message    消息体，Object类型
     */
    public void sendOneWay(String topic, String tag, Object message) {
        Message rocketmqMsg = doConvert(topic, tag, message, null);
        sendOneWay(topic, tag, rocketmqMsg);
    }

    /**
     * 单向传输消息，不需要等待broker的消息确认
     * @param topic          主题
     * @param tag             标签
     * @param rocketmqMsg    rocketmq message
     */
    private void sendOneWay(String topic, String tag, Message rocketmqMsg) {
        if(topic==null || "".equals(topic)){
            throw new IllegalArgumentException("'topic' cannot be null");
        }
        if(rocketmqMsg==null || rocketmqMsg.getBody()==null || rocketmqMsg.getBody().length<=0){
            throw new IllegalArgumentException("'message' and 'message.body' cannot be null");
        }

        try {
            producer.sendOneway(rocketmqMsg);
        }
        catch (Exception e) {
            logger.error("sendOneWay failed. topic:{}, tag:{}, messageBody:{} ", topic, tag, rocketmqMsg.getBody());
            throw new MessagingException(e.getMessage(), e);
        }
    }

}

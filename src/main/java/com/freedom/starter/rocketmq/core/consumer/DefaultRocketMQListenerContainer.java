package com.freedom.starter.rocketmq.core.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.freedom.starter.rocketmq.enums.ConsumeMode;
import com.freedom.starter.rocketmq.enums.SelectorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

/**
 * 整合rocketmq consumer监听消息 和 调用对应的RocketMQListener的onMessage()方法处理消费消息的逻辑
 */
public class DefaultRocketMQListenerContainer implements InitializingBean, DisposableBean {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRocketMQListenerContainer.class);

    /** 消费者 */
    private DefaultMQPushConsumer consumer;

    /** 消费监听接口实现 */
    private RocketMQListener rocketMQListener;

    /** consumer是否启动标示 */
    private volatile boolean started;

    /** 消费组 */
    private String consumerGroup;

    /** name server */
    private String nameServer;

    /** 主题 */
    private String topic;

    /** 从哪儿开始消费 */
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /** 消费模式，默认是并发消费 */
    private ConsumeMode consumeMode = ConsumeMode.CONCURRENTLY;

    /** 消息模式，默认集群模式 */
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /** 过滤类型，使用TAG过滤，4.1.0-incubating开始支持SQL92 */
    private SelectorType selectorType = SelectorType.TAG;

    /** 过滤表达式 */
    private String selectorExpress = "*";

    /** 最大消费线程数，默认20 */
    private int consumeThreadMin = 20;

    /** 最大消费线程数，默认64 */
    private int consumeThreadMax = 64;

    /** 最大批量消费消息数量，默认值1 */
    private int consumeMessageBatchMaxSize = 1;

    /** 最大重复消费次数，默认值1 */
    private int maxReconsumeTime = 3;


    /**
     * Message consume retry strategy
     * -1，no retry,put into DLQ directly
     * 0，broker control retry frequency
     * >0，client control retry frequency
     */
    private int delayLevelWhenNextConsume = 0;

    /** 顺序消费后，返回SUSPEND_CURRENT_QUEUE_A_MOMENT状态，
     *  暂停当前队列一段时间再重试，最多重试16次
     *  暂停的时间默认1000ms
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /** rocketMQListener监听接口需要的消息类型 */
    private Class messageType;

    private String charset = "UTF-8";



    public void setRocketMQListener(RocketMQListener rocketMQListener) {
        this.rocketMQListener = rocketMQListener;
    }
    public boolean isStarted() {
        return started;
    }
    public void setStarted(boolean started) {
        this.started = started;
    }
    public String getConsumerGroup() {
        return consumerGroup;
    }
    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
    public String getNameServer() {
        return nameServer;
    }
    public void setNameServer(String nameServer) {
        this.nameServer = nameServer;
    }
    public String getTopic() {
        return topic;
    }
    public void setTopic(String topic) {
        this.topic = topic;
    }
    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }
    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
    public ConsumeMode getConsumeMode() {
        return consumeMode;
    }
    public void setConsumeMode(ConsumeMode consumeMode) {
        this.consumeMode = consumeMode;
    }
    public MessageModel getMessageModel() {
        return messageModel;
    }
    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }
    public SelectorType getSelectorType() {
        return selectorType;
    }
    public void setSelectorType(SelectorType selectorType) {
        this.selectorType = selectorType;
    }
    public String getSelectorExpress() {
        return selectorExpress;
    }
    public void setSelectorExpress(String selectorExpress) {
        this.selectorExpress = selectorExpress;
    }
    public int getConsumeThreadMin() {
        return consumeThreadMin;
    }
    public void setConsumeThreadMin(int consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }
    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }
    public void setConsumeThreadMax(int consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }
    public int getConsumeMessageBatchMaxSize() {
        return consumeMessageBatchMaxSize;
    }
    public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
        this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
    }
    public int getMaxReconsumeTime() {
        return maxReconsumeTime;
    }
    public void setMaxReconsumeTime(int maxReconsumeTime) {
        this.maxReconsumeTime = maxReconsumeTime;
    }
    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }
    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }
    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }
    public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }
    public String getCharset() {
        return charset;
    }
    public void setCharset(String charset) {
        this.charset = charset;
    }


    /**
     * 实现InitializingBean接口的方法
     * 在所有属性设置完成后，由BeanFactory调用此方法
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        start();  //调用启动方法，涉及
    }

    /**
     * 实现DisposableBean接口的方法
     * 在BeanFactory销毁Bean之前释放资源
     * @throws Exception
     */
    @Override
    public void destroy() throws Exception {
        consumer.shutdown();
    }



    /**
     * 启动
     * @throws MQClientException
     */
    public synchronized void start() throws MQClientException {
        //是否已经启动
        if (this.isStarted()) {
            throw new IllegalStateException("container already started. " + this.toString());
        }

        //初始化consumer
        initRocketMQPushConsumer();

        // 获取需要转换的消息类型
        this.messageType = getMessageType();
        logger.debug("msgType: {}", messageType.getName());

        //启动rocketmq consumer
        consumer.start();

        //设置启动标示
        this.setStarted(true);

        logger.info("started container: {}", this.toString());
    }

    /**
     * 初始化consumer
     */
    private void initRocketMQPushConsumer() throws MQClientException {
        //校验
        Assert.notNull(rocketMQListener, "Property 'rocketMQListener' is required");
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(nameServer, "Property 'nameServer' is required");
        Assert.notNull(topic, "Property 'topic' is required");


        consumer = new DefaultMQPushConsumer(consumerGroup);

        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeFromWhere(consumeFromWhere);  //从哪儿开始消费，默认CONSUME_FROM_LAST_OFFSET

        consumer.setConsumeThreadMax(consumeThreadMax);  //最大消费线程数
        //如果最小消费线程 大于 最大消费线程，设置为最大消费线程
        if(consumeThreadMin > consumeThreadMax){
            consumer.setConsumeThreadMin(consumeThreadMax);
        }
        else{
            consumer.setConsumeThreadMin(consumeThreadMin);
        }

        consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);  //最大批量消息大小，默认值1

        consumer.setMessageModel(messageModel);  //默认集群模式


        //设置过滤方式
        switch (selectorType) {
            case TAG:
                consumer.subscribe(topic, selectorExpress);
                break;
            default:
                throw new IllegalArgumentException("Property 'selectorType' was wrong.");
        }

        //根据消费模式设置MessageListener实现
        switch (consumeMode) {
            case CONCURRENTLY:  //并发消费
                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                break;
            case ORDERLY:  //顺序消费
                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }
    }

    /**
     * 获取RocketMQListener接口的类型
     * @return
     */
    private Class getMessageType() {
        //得到目标类实现的接口对应的Type对象，带泛型信息  RocketMQListener<T>
        Type[] interfaces = rocketMQListener.getClass().getGenericInterfaces();

        if (interfaces != null) {
            for (Type type : interfaces) {
                //ParameterizedType表示参数化的类型，比如Collection<String>
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;

                    //如果接口原始类型是RocketMQListener
                    if (Objects.equals(parameterizedType.getRawType(), RocketMQListener.class)) {
                        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();  //实际的泛型参数类型
                        if (actualTypeArguments!=null && actualTypeArguments.length > 0) {
                            return (Class) actualTypeArguments[0];  //返回RocketMQListener接口的泛型参数类型
                        }
                        else {
                            return Object.class;
                        }
                    }
                }
            }

            return Object.class;
        }
        else {
            return Object.class;
        }
    }

    //public static void main(String[] args){
    //    DefaultRocketMQListenerContainer container = new DefaultRocketMQListenerContainer();
    //    container.setRocketMQListener(new TestRcoektMQListener());
    //
    //    Object clazz = container.getMessageType();
    //    System.out.println(clazz);
    //}
    //
    //static class TestRcoektMQListener implements RocketMQListener<String> {
    //    @Override
    //    public void onMessage(String message) {
    //
    //    }
    //}


    /**
     * 做MessageExt到RocketMQListener指定的messageType的类型转换
     * @param messageExt
     * @return
     */
    private Object doConvertMessage(MessageExt messageExt) {
        //如果需要的消息类型是MessageExt，直接返回
        if(Objects.equals(messageType, MessageExt.class)){
            return messageExt;
        }
        else {
            String messageStr = new String(messageExt.getBody(), Charset.forName(charset));

            //如果需要的类型是字符串
            if(Objects.equals(messageType, String.class)){
                return messageStr;
            }
            //否则按照json转换
            else {
                return JSON.parseObject(messageStr, messageType);
            }
        }
    }


    /**
     * 并发消费MessageListenerConcurrently的默认实现类
     */
    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for(MessageExt messageExt : msgs) {
                try {
                    logger.debug("received msg: {}", messageExt);

                    long now = System.currentTimeMillis();
                    rocketMQListener.onMessage(doConvertMessage(messageExt));
                    long costTime = System.currentTimeMillis() - now;
                    logger.info("consume success. msgId: {} cost: {} ms", messageExt.getMsgId(), costTime);
                }
                catch (Exception e){
                    context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);  //默认值0，broker control retry frequency

                    //没到最大重试次数，返回RECONSUME_LATER
                    if(messageExt.getReconsumeTimes() <= maxReconsumeTime) {
                        logger.error("consume failed, recomsume time[" + messageExt.getReconsumeTimes() + "], messageExt:" + messageExt, e);
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                    //达到最大重试次数，返回CONSUME_SUCCESS，可结合日志收集告警人工方式排查问题
                    else {
                        logger.error("consume failed, reach the maximum number of retries[" + maxReconsumeTime + "]. messageExt:" + messageExt, e);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    /**
     * 顺序消费MessageListenerOrderly的默认实现类
     */
    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            for(MessageExt messageExt : msgs){
                logger.debug("received msg: {}", messageExt);
                try{
                    long now = System.currentTimeMillis();
                    rocketMQListener.onMessage(doConvertMessage(messageExt));
                    long costTime = System.currentTimeMillis() - now;
                    logger.debug("consume {} cost: {} ms", messageExt.getMsgId(), costTime);
                }
                catch(Exception e){
                    context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);  //暂停默认1000ms

                    //没到最大重试次数，返回SUSPEND_CURRENT_QUEUE_A_MOMENT
                    if(messageExt.getReconsumeTimes() <= maxReconsumeTime) {
                        logger.error("consume failed, recomsume time[" + messageExt.getReconsumeTimes() + "], messageExt:" + messageExt, e);
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;  //暂停当前队列一段时间再重试，最多重试16次
                    }
                    //达到最大重试次数，返回SUCCESS，可结合日志收集告警人工方式排查问题
                    else {
                        logger.error("consume failed, reach the maximum number of retries[" + maxReconsumeTime + "]. messageExt:" + messageExt, e);
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

}

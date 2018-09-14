package com.freedom.starter.rocketmq.annotation;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.freedom.starter.rocketmq.enums.ConsumeMode;
import com.freedom.starter.rocketmq.enums.SelectorType;
import org.springframework.stereotype.Service;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)  //注解的使用范围，用于描述类、接口(包括注解类型) 或enum声明
@Retention(RetentionPolicy.RUNTIME)  //注解的生命周期，在运行时有效
@Documented
@Service  //注册到spring容器
public @interface RocketMQMessageListener {

    /**
     * 消费组
     */
    String consumerGroup();

    /**
     * Topic
     */
    String topic();

    /**
     * 从哪儿开始消费，默认CONSUME_FROM_LAST_OFFSET
     */
    ConsumeFromWhere consumeFromWhere()  default ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    /**
     * 过滤消息类型，只有TAG方式，4.1.0-incubating开始支持SQL92
     */
    SelectorType selectorType() default SelectorType.TAG;

    /**
     * 过滤消息表达式，默认*
     */
    String selectorExpress() default "*";

    /**
     * 消费模式，默认是并发消费CONCURRENTLY
     */
    ConsumeMode consumeMode() default ConsumeMode.CONCURRENTLY;

    /**
     * 消息模式，默认集群模式，还有BROADCASTING广播模式
     */
    MessageModel messageModel() default MessageModel.CLUSTERING;

    /**
     * 最小消费线程数，默认20
     */
    int consumeThreadMin()  default  20;

    /**
     * 最大消费线程数，默认64
     */
    int consumeThreadMax() default 64;

    /**
     * 最大批量消费大小，默认1
     */
    int consumeMessageBatchMaxSize() default 1;
}

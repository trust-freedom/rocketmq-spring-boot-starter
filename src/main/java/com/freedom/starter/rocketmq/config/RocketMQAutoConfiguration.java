package com.freedom.starter.rocketmq.config;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.freedom.starter.rocketmq.annotation.EnableRocketMQ;
import com.freedom.starter.rocketmq.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

@Configuration
@ConditionalOnBean(annotation = EnableRocketMQ.class)     //ApplicationContext中有Bean使用@EnableRocketMQ时，配置生效
@EnableConfigurationProperties(RocketMQProperties.class)  //使RocketMQProperties生效，加入spring容器中
public class RocketMQAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQAutoConfiguration.class);

    @Value("${spring.application.name:}")
    private String springApplicationName;


    /**
     * 创建生产者Producer
     * @param rocketMQProperties
     * @return
     */
    @Bean
    @ConditionalOnClass(DefaultMQProducer.class)  //类路径下有DefaultMQProducer.class
    @ConditionalOnMissingBean(DefaultMQProducer.class)  //spring容器中还未注册DefaultMQProducer实例
    //@ConditionalOnProperty(prefix = "spring.rocketmq", name = {"name-server", "producer.group"})  //有以spring.rockermq为前缀的nameServer、producer.group配置
    public DefaultMQProducer rocketmqProducer(RocketMQProperties rocketMQProperties){
        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
        String groupName = producerConfig!=null ? producerConfig.getGroup() : "";  //生产组

        /**
         * 校验：
         *   nameServer不能为空
         *   producer.group或spring.application.name二者至少有一个
         */
        Assert.hasText(rocketMQProperties.getNameServer(), "[spring.rocketmq.name-server] must not be null");
        if(!StringUtils.hasText(groupName) && !StringUtils.hasText(springApplicationName)){
            throw new IllegalArgumentException("[spring.rocketmq.producer.group] or [spring.application.name] can not both null");
        }


        if(!StringUtils.hasText(groupName)){
            groupName = springApplicationName;  //如果没有设置spring.rocketmq.producer.group，默认使用spring.application.name
        }

        //创建Producer
        DefaultMQProducer rocketmqProducer = new DefaultMQProducer(groupName);
        rocketmqProducer.setNamesrvAddr(rocketMQProperties.getNameServer());  //nameServer
        rocketmqProducer.setVipChannelEnabled(producerConfig.isVipChannelEnabled());  //是否启用vip通道，默认值false
        rocketmqProducer.setSendMsgTimeout(producerConfig.getSendMsgTimeout());  //发送消息超时时间，单位毫秒，默认值3000
        rocketmqProducer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
        rocketmqProducer.setRetryTimesWhenSendAsyncFailed(producerConfig.getRetryTimesWhenSendAsyncFailed());
        rocketmqProducer.setMaxMessageSize(producerConfig.getMaxMessageSize());  //消息体最大值，单位byte，默认4Mb
        rocketmqProducer.setCompressMsgBodyOverHowmuch(producerConfig.getCompressMsgBodyOverHowmuch());  //压缩消息体的阀值，默认1024 * 4，4k，即默认大于4k的消息体将开启压缩
        rocketmqProducer.setRetryAnotherBrokerWhenNotStoreOK(producerConfig.isRetryAnotherBrokerWhenNotStoreOk());  //内部发送失败时是否重试另一个broker

        logger.info("DefaultMQProducer初始化完成： " + rocketmqProducer);

        return rocketmqProducer;
    }


    /**
     * 创建RocketMQTemplate
     * @param producer
     * @return
     */
    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean(RocketMQTemplate.class)
    public RocketMQTemplate rocketMQTemplate(DefaultMQProducer producer){
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        rocketMQTemplate.setProducer(producer);

        return rocketMQTemplate;
    }

}

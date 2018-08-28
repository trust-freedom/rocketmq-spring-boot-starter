package com.freedom.starter.rocketmq.config;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.freedom.starter.rocketmq.annotation.EnableRocketMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnBean(annotation = EnableRocketMQ.class)     //ApplicationContext中有Bean使用@EnableRocketMQ时，配置生效
@EnableConfigurationProperties(RocketMQProperties.class)  //使RocketMQProperties生效，加入spring容器中
public class RocketMQAutoConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQAutoConfiguration.class);


    @Bean
    @ConditionalOnClass(DefaultMQProducer.class)  //类路径下有DefaultMQProducer.class
    @ConditionalOnMissingBean(DefaultMQProducer.class)  //spring容器中还未注册DefaultMQProducer实例
    @ConditionalOnProperty(prefix = "spring.rocketmq", name = {"name-server", "producer.group"})  //有以spring.rockermq为前缀的nameServer、producer.group配置
    public DefaultMQProducer rocketmqProducer(RocketMQProperties rocketMQProperties){
        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();

        String groupName = producerConfig.getGroup();  //生产组
        //TODO 如果没有定义生产组

        DefaultMQProducer rocketmqProducer = new DefaultMQProducer(groupName);
        rocketmqProducer.setNamesrvAddr(rocketMQProperties.getNameServer());

        logger.info("DefaultMQProducer初始化完成： " + rocketmqProducer);

        return rocketmqProducer;
    }

}

package com.freedom.starter.rocketmq.config;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.freedom.starter.rocketmq.annotation.EnableRocketMQ;
import com.freedom.starter.rocketmq.annotation.RocketMQMessageListener;
import com.freedom.starter.rocketmq.core.consumer.DefaultRocketMQListenerContainer;
import com.freedom.starter.rocketmq.core.consumer.RocketMQListener;
import com.freedom.starter.rocketmq.core.producer.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;



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


    /**
     * 消费者的RocketMQListener相关配置
     */
    @Configuration
    @ConditionalOnClass(DefaultMQPushConsumer.class)
    @EnableConfigurationProperties(RocketMQProperties.class)  //不添加@Autowired RocketMQProperties报错
    public static class ListenerContainerConfiguration implements ApplicationContextAware, InitializingBean {

        private ConfigurableApplicationContext applicationContext;

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = (ConfigurableApplicationContext)applicationContext;
        }

        @Autowired
        private RocketMQProperties rocketMQProperties;

        @Autowired
        private StandardEnvironment environment;

        //生成container beanName时的计数器
        private AtomicLong counter = new AtomicLong(0);

        /**
         * 实现InitializingBean接口的方法
         * 在所有属性设置完成后，由BeanFactory调用此方法
         * 用于向sping注册所有标注了@RocketMQMessageListener注解的实现，即消息订阅者
         * @throws Exception
         */
        @Override
        public void afterPropertiesSet() throws Exception {
            Assert.hasText(rocketMQProperties.getNameServer(), "[spring.rocketmq.name-server] must not be null");

            //获取所有使用了@RocketMQMessageListener注解的spring容器中的bean
            Map<String, Object> beans = applicationContext.getBeansWithAnnotation(RocketMQMessageListener.class);

            //迭代并调用registerContainer()，向spring中注册RocketMQListenerContainer
            if(beans!=null && beans.keySet()!=null && !beans.keySet().isEmpty()){
                for(String beanName : beans.keySet()){
                    registerContainer(beanName, beans.get(beanName));
                }
            }
        }

        /**
         * 使用@RocketMQMessageListener的bean，向spring容器中注册RocketMQListenerContainer
         * @param beanName
         * @param bean
         */
        private void registerContainer(String beanName, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);  //获取bean的Class

            //判断clazz是不是RocketMQListener接口类型的
            if (!RocketMQListener.class.isAssignableFrom(clazz)) {
                throw new IllegalStateException(clazz + " is not instance of " + RocketMQListener.class.getName());
            }

            RocketMQListener rocketMQListener = (RocketMQListener) bean;
            RocketMQMessageListener annotation = clazz.getAnnotation(RocketMQMessageListener.class);//获取RocketMQListener接口实现类上的注解RocketMQMessageListener

            /**
             * BeanDefinition的builder，用于创建DefaultRocketMQListenerContainer的BeanDefinition
             * environment.resolvePlaceholders()用于处理${}占位符的问题，即@RocketMQMessageListener注解上的属性部分可以使用${}占位符，从配置文件获取
             */
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultRocketMQListenerContainer.class);
            beanBuilder.addPropertyValue("nameServer", rocketMQProperties.getNameServer());
            beanBuilder.addPropertyValue("topic", environment.resolvePlaceholders(annotation.topic()));
            beanBuilder.addPropertyValue("consumerGroup", environment.resolvePlaceholders(annotation.consumerGroup()));
            beanBuilder.addPropertyValue("consumeMode", annotation.consumeMode()); //CONCURRENTLY 或 ORDERLY
            beanBuilder.addPropertyValue("consumeThreadMin", annotation.consumeThreadMin()); //最小消费线程数，默认20
            beanBuilder.addPropertyValue("consumeThreadMax", annotation.consumeThreadMax()); //最大消费线程数，默认64
            beanBuilder.addPropertyValue("consumeMessageBatchMaxSize", annotation.consumeMessageBatchMaxSize()); //最大批量消费大小，默认1
            beanBuilder.addPropertyValue("maxReconsumeTime", annotation.maxReconsumeTime()); //最大重复消费次数，默认3
            beanBuilder.addPropertyValue("messageModel", annotation.messageModel());  //CLUSTERING 或 BROADCASTING
            beanBuilder.addPropertyValue("selectorType", annotation.selectorType());  //过滤类型，只有TAG
            beanBuilder.addPropertyValue("selectorExpress", environment.resolvePlaceholders(annotation.selectorExpress())); //过滤表达式
            beanBuilder.addPropertyValue("rocketMQListener", rocketMQListener);  //rocketMQListener实现类的实例
            beanBuilder.setDestroyMethodName("destroy");

            /**
             * 创建bean的定义BeanDefinition，并给bean起名，之后注册到beanFactory
             * 过程类似于在xml中配置
             */
            String containerBeanName = String.format("%s_%s", DefaultRocketMQListenerContainer.class.getName(), counter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();

            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            //获取名为containerBeanName的DefaultRocketMQListenerContainer实例
            DefaultRocketMQListenerContainer container = beanFactory.getBean(containerBeanName, DefaultRocketMQListenerContainer.class);

            //启动container
            if (!container.isStarted()) {
                try {
                    container.start();
                }
                catch (Exception e) {
                    logger.error("started container failed. {}", container, e);
                    throw new RuntimeException(e);
                }
            }

            logger.info("register rocketmq listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
        }
    }


}

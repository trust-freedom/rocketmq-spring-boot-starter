package com.freedom.starter.rocketmq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties( prefix = "spring.rocketmq")
public class RocketMQProperties {

    /**
     * name server for rocketmq
     * formats: `host:port;host:port`
     */
    private String nameServer;

    /**
     * producer group 生产组
     */
    private Producer producer;


    public static class Producer {

        /** 生产组 */
        private String group;




        public String getGroup() {
            return group;
        }
        public void setGroup(String group) {
            this.group = group;
        }
    }


    public String getNameServer() {
        return nameServer;
    }
    public void setNameServer(String nameServer) {
        this.nameServer = nameServer;
    }
    public Producer getProducer() {
        return producer;
    }
    public void setProducer(Producer producer) {
        this.producer = producer;
    }
}

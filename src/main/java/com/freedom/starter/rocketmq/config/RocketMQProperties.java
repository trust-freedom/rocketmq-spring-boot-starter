package com.freedom.starter.rocketmq.config;

import org.springframework.beans.factory.annotation.Value;
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
    private Producer producer = new Producer(); //默认为空的Producer，否则application.yml中没有相关配置producer为null


    /**
     * Producer参数
     */
    public static class Producer {

        /** 生产组 */
        private String group;

        /** 是否vip通道，默认值false */
        private boolean vipChannelEnabled = false;

        /**
         * 发送消息超时时间，单位毫秒，默认值3000
         */
        private int sendMsgTimeout = 3000;

        /**
         * 压缩消息体的阀值，默认1024 * 4，4k，即默认大于4k的消息体将开启压缩
         */
        private int compressMsgBodyOverHowmuch = 1024 * 4;

        /**
         * 在同步模式下，声明发送失败之前内部执行的最大重试次数
         * 这可能会导致消息重复，应用程序开发人员需要解决此问题
         */
        private int retryTimesWhenSendFailed = 2;

        /**
         * 在异步模式下，声明发送失败之前内部执行的最大重试次数
         * 这可能会导致消息重复，应用程序开发人员需要解决此问题
         */
        private int retryTimesWhenSendAsyncFailed = 2;

        /**
         * 内部发送失败时是否重试另一个broker
         */
        private boolean retryAnotherBrokerWhenNotStoreOk = false;

        /**
         * 消息体最大值，单位byte，默认4Mb
         */
        private int maxMessageSize = 1024 * 1024 * 4; // 4M


        public String getGroup() {
            return group;
        }
        public void setGroup(String group) {
            this.group = group;
        }
        public boolean isVipChannelEnabled() {
            return vipChannelEnabled;
        }
        public void setVipChannelEnabled(boolean vipChannelEnabled) {
            this.vipChannelEnabled = vipChannelEnabled;
        }
        public int getSendMsgTimeout() {
            return sendMsgTimeout;
        }
        public void setSendMsgTimeout(int sendMsgTimeout) {
            this.sendMsgTimeout = sendMsgTimeout;
        }
        public int getCompressMsgBodyOverHowmuch() {
            return compressMsgBodyOverHowmuch;
        }
        public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
            this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
        }
        public int getRetryTimesWhenSendFailed() {
            return retryTimesWhenSendFailed;
        }
        public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
            this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
        }
        public int getRetryTimesWhenSendAsyncFailed() {
            return retryTimesWhenSendAsyncFailed;
        }
        public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed) {
            this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
        }
        public boolean isRetryAnotherBrokerWhenNotStoreOk() {
            return retryAnotherBrokerWhenNotStoreOk;
        }
        public void setRetryAnotherBrokerWhenNotStoreOk(boolean retryAnotherBrokerWhenNotStoreOk) {
            this.retryAnotherBrokerWhenNotStoreOk = retryAnotherBrokerWhenNotStoreOk;
        }
        public int getMaxMessageSize() {
            return maxMessageSize;
        }
        public void setMaxMessageSize(int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
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

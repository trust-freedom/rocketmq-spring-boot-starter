package com.freedom.starter.rocketmq.message;

import java.io.Serializable;

/**
 * 封装RocketMQ消息头信息
 */
public class RocketMQHeader implements Serializable {
    /**  */
    private String keys = "";


    /** 默认值true */
    private boolean waitStoreMsgOK = true;



    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public boolean isWaitStoreMsgOK() {
        return waitStoreMsgOK;
    }

    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.waitStoreMsgOK = waitStoreMsgOK;
    }
}

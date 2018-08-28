package com.freedom.starter.rocketmq.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)  //注解的使用范围，用于描述类、接口(包括注解类型) 或enum声明
@Retention(RetentionPolicy.RUNTIME)  //注解的生命周期，在运行时有效
@Documented
@Inherited
public @interface EnableRocketMQ {
}

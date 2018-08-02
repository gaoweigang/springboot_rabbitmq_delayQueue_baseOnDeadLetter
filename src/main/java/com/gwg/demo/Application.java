package com.gwg.demo;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
/**
 * @author gaoweigang
 * 
 * 我们的Application类上使用的第一个注解是@RestController。这被称为一个构造型注解。
 * 它为阅读代码的人们提供建议。对于Spring,该类扮演了一个特殊角色。在本示例中，我们的
 * 类是一个web @Controller,所以当处理进来的web请求时，Spring会询问他.
 * 
 * @SpringBootApplication 等价于 @Configuration, @EnableAntoConfiguration 和 @ComponentScan。
 * 
 */
@SpringBootApplication//启动了自动配置，如果配置文件中有数据库的配置，则会自动创建dataSource
public class Application{
		
	public static void main(String[] args) throws Exception{
		SpringApplication.run(Application.class, args);
				
	}

}

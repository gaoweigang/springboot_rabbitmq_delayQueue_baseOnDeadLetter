package com.gwg.demo;

/**
 * 
 * https://blog.csdn.net/wgw335363240/article/details/5854402
 *这个方法的意思就是在jvm中增加一个关闭的钩子，当jvm关闭的时候，会执行系统中已经设置的所有通过方法addShutdownHook
 *添加的钩子，当系统执行完这些钩子后，jvm才会关闭。所以这些钩子可以在jvm关闭的时候进行内存清理、对象销毁等操作。
 */
public class RunTimeTest {

}

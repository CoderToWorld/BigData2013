package com.suyao.maven;

import org.junit.Test;

/**
 * @author suyso
 * @create 2020-03-30 18:50
 */
public class HelloTest {
    @Test
    public void testHello(){
        Hello hello = new Hello();
        String maven = hello.sayHello("Maven");
        System.out.println(maven);
    }
}

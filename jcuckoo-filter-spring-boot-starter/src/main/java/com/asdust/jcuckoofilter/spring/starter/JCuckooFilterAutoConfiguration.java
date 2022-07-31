package com.asdust.jcuckoofilter.spring.starter;


import com.asdust.JCuckooFilter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author: chordCreater
 * @date: 2022/7/31 17:35
 * @desc:
 */
@Configuration
@ConditionalOnClass()
public class JCuckooFilterAutoConfiguration {


    // 自动配置CuckooFilter
    @Bean
    public JCuckooFilter jCuckooFilter(){
        return null;
    }

}
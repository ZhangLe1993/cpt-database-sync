package com.aihuishou.bi;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class Config {

    @Bean(name = "sourceDataSource")
    @ConfigurationProperties(prefix = "spring.source")
    public DataSource sourceDataSource() {
        return new DruidDataSource();
    }

    @Bean(name = "targetDataSource")
    @ConfigurationProperties(prefix = "spring.target")
    public DataSource targetDataSource() {
        return new DruidDataSource();
    }
}

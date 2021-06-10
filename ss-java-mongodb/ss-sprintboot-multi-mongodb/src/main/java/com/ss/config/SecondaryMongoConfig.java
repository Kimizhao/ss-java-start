package com.ss.config;

import com.ss.config.props.MultipleMongoProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;


@Configuration
@EnableConfigurationProperties(MultipleMongoProperties.class)
@EnableMongoRepositories(basePackages = "com.ss.repository.secondary",
		mongoTemplateRef = "secondaryMongoTemplate")
public class SecondaryMongoConfig {

}

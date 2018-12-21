package com.epam.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Container application.
 */
@Configuration
@EnableAutoConfiguration
public class ContainerApplication {

    /**
     * Entry point into our container application.
     *
     * @param args cmd args.
     */
    public static void main(String[] args) {
        SpringApplication.run(ContainerApplication.class, args);
    }

    /**
     * Gets the bean that executes all the necessary actions.
     *
     * @return the bean from the description.
     */
    @Bean
    public JobPojo jobPojo() {
        return new JobPojo();
    }

}

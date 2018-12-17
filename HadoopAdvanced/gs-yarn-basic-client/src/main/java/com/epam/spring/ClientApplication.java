package com.epam.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.yarn.client.YarnClient;

/**
 * Yarn client.
 */
@EnableAutoConfiguration
public class ClientApplication {

	/**
	 * Entry point into the yarn client.
	 * @param args cmd args.
	 */
	public static void main(String[] args) {
		SpringApplication.run(ClientApplication.class, args)
			.getBean(YarnClient.class)
			.submitApplication();
	}

}

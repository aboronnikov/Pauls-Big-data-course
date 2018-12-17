package com.epam.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

/**
 * Application master for this program.
 */
@EnableAutoConfiguration
public class AppmasterApplication {

	/**
	 * Entry point into the application master.
	 * @param args cmd args.
	 */
	public static void main(String[] args) {
		SpringApplication.run(AppmasterApplication.class, args);
	}

}

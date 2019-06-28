package com.ylc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class WebServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebServiceApplication.class, args);
    }

}

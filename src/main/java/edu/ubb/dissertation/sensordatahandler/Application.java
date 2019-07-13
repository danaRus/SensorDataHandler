package edu.ubb.dissertation.sensordatahandler;

import org.apache.log4j.BasicConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        BasicConfigurator.configure();
        SpringApplication.run(Application.class, args);
    }
}

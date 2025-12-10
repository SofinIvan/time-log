package ru.time.timelog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TimeLogApp {
    public static void main(String[] args) {
        SpringApplication.run(TimeLogApp.class, args);
    }
}

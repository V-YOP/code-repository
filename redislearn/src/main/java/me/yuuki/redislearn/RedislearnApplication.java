package me.yuuki.redislearn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
@RequestMapping("/")
public class RedislearnApplication {
	@GetMapping("/")
	String sayHello() {
		return "Hello, Happy World!";
	}
	public static void main(String[] args) {
		SpringApplication.run(RedislearnApplication.class, args);
	}
}


package br.com.wmw.gatewaysankhya;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

@SpringBootApplication
@ImportResource("classpath:gateway-sankhya-config.xml")
public class GatewaySankhyaApplication {

	public static void main(String[] args) {
		SpringApplication.run(GatewaySankhyaApplication.class, args);
	}
	
}

package com.thinkmicroservices.nats.jetstream.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.nativex.hint.TypeHint;

@ComponentScan(basePackages = "com.thinkmicroservices.nats.jetstream.client")
/* include the Spring Native @TypeHint to enable NATS SocketDataPort inclusion 
in executable image*/
@TypeHint(types = io.nats.client.impl.SocketDataPort.class, typeNames = "io.nats.client.impl.SocketDataPort")
@SpringBootApplication
public class JetstreamClientServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(JetstreamClientServiceApplication.class, args);
	}

}

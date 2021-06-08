package com.ss.netty.udpclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class UdpClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(UdpClientApplication.class, args);
        UdpClient udpClient = new UdpClient();
        udpClient.start();
    }

}

package com.ds.rabbitmq;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Config {

    public static Connection createRemoteConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        //set IP here instead of 'localhost'
        factory.setHost("localhost");
        return factory.newConnection();
    }
}

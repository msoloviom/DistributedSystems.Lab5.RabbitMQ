package com.ds.rabbitmq.hello;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.util.concurrent.TimeoutException;

import static com.ds.rabbitmq.Config.createRemoteConnection;

public class Send {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws java.io.IOException, TimeoutException {
        Connection connection = createRemoteConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        //for (int i = 0; i < 20; i++) {
            String message = 1 + "";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        //}

        channel.close();
        connection.close();

    }
}

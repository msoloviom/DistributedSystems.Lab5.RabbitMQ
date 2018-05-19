package com.ds.rabbitmq.hello;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static com.ds.rabbitmq.Config.createRemoteConnection;

public class RecvWithAck {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] argv) throws Exception {

        Connection connection = createRemoteConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                long deliveryTag = envelope.getDeliveryTag();
                channel.basicReject(deliveryTag, true);
                try {
                    System.out.println(" [x] First consumer started processing of message: " + message);
                    System.out.println(" [x] Ooops! Connection closed :(");
                    connection.close();
                    channel.close();
                    Thread.sleep(100000);
                } catch (TimeoutException | InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(QUEUE_NAME, false, "a-consumer-tag", consumer);
    }
}

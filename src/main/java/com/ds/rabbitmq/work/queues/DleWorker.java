package com.ds.rabbitmq.work.queues;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

import static com.ds.rabbitmq.Config.createRemoteConnection;
import static com.ds.rabbitmq.work.queues.NewTask.DLE_QUEUE;

public class DleWorker {
    public static void main(String[] argv) throws Exception {
        Connection connection = createRemoteConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(DLE_QUEUE, false, false, false, null);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.err.println(" [x] DLE queue received '" + message + "'");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        channel.basicConsume(DLE_QUEUE, true, consumer);

    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

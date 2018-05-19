package com.ds.rabbitmq.work.queues;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.ds.rabbitmq.Config.createRemoteConnection;
import static com.ds.rabbitmq.work.queues.NewTask.EXCHANGE_NAME;
import static com.ds.rabbitmq.work.queues.NewTask.LIM_QUEUE;

public class MainWorker {

    public static void main(String[] argv) throws Exception {
        Connection connection = createRemoteConnection();
        Channel channel = connection.createChannel();

        Map<String, Object> map = new HashMap();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        map.put("x-dead-letter-exchange", EXCHANGE_NAME);
        map.put("x-max-length", 5);

        channel.queueDeclare(LIM_QUEUE, false, false, false, map);
        // Fair dispatch
        //channel.basicQos(1);

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.err.println(" [x] Main queue received '" + message + "'");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        channel.basicConsume(LIM_QUEUE, true, consumer);

    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

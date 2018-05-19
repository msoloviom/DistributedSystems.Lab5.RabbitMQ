package com.ds.rabbitmq.crossmssging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;

import static com.ds.rabbitmq.Config.createRemoteConnection;

public class Worker {
    private static final String TASK_QUEUE_NAME = "task_queue";
    private static final String RESP_QUEUE_NAME = "resp_queue";

    public static void main(String[] argv) throws Exception {
        Connection connection = createRemoteConnection();
        Channel channel = connection.createChannel();

        /* Two things are required to make sure that messages aren't lost:
         * we need to mark both the queue and messages as durable.
         */
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // Fair dispatch
        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");

                System.out.println(" [x] Received '" + message + "'");
                try {
                    doWork(message);
                } finally {
                    System.out.println(" [x] Done");
                    /*
                     * There aren't any message timeouts; RabbitMQ will redeliver the message
                     * when the consumer dies.
                     * It's fine even if processing a message takes a very, very long time.
                     */
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
                channel.queueDeclare(RESP_QUEUE_NAME, true, false, false, null);

                String messg = "!!! Changed: " + message;
                channel.basicPublish("", RESP_QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        messg.getBytes());
                System.out.println(" [x] Sent '" + messg + "'");
            }
        };
        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

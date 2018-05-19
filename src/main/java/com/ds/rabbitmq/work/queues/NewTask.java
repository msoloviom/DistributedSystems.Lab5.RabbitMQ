package com.ds.rabbitmq.work.queues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.ds.rabbitmq.Config.createRemoteConnection;

public class NewTask {

    public static final String LIM_QUEUE = "lim_queue";
    public static final String DLE_QUEUE = "dle_queue";

    public static final String EXCHANGE_NAME = "lim_exc";

    public static void main(String[] argv) throws java.io.IOException, TimeoutException {
        Connection connection = createRemoteConnection();
        Channel channel = connection.createChannel();


        channel.queueDeclare(DLE_QUEUE, false, false, false, null);

        Map<String, Object> map = new HashMap();
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        map.put("x-dead-letter-exchange", EXCHANGE_NAME);
        map.put("x-max-length", 5);

        channel.queueBind(DLE_QUEUE, EXCHANGE_NAME, "");


        channel.queueDeclare(LIM_QUEUE, false, false, false, map);

        for (int i = 0; i < 10; i++) {
            String message = i + " " + getMessage(argv);
            channel.basicPublish("", LIM_QUEUE, null, message.getBytes());
            System.out.println(" Sent: '" + message + "'");
        }

        /*Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.err.println(" [x] Was not received '" + message + "'");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        channel.basicConsume(DLE_QUEUE, true, consumer);*/

        channel.close();
        connection.close();
    }


    private static String getMessage(String[] strings) {
        if (strings.length < 1)
            return "Hello World!";
        return joinStrings(strings, " ");
    }

    private static String joinStrings(String[] strings, String delimiter) {
        int length = strings.length;
        if (length == 0) return "";
        StringBuilder words = new StringBuilder(strings[0]);
        for (int i = 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }

}


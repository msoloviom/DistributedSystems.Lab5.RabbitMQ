package com.ds.rabbitmq.crossmssging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;

import java.util.concurrent.TimeoutException;

import static com.ds.rabbitmq.Config.createRemoteConnection;

public class NewTask {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws java.io.IOException, TimeoutException {
        Connection connection = createRemoteConnection();
        Channel channel = connection.createChannel();

        /* Two things are required to make sure that messages aren't lost:
         * we need to mark both the queue and messages as durable.
         */
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        for (int i = 0; i < 1000; i++) {
        String message = i + " " + getMessage(argv);
            channel.basicPublish("", TASK_QUEUE_NAME,
                    MessageProperties.PERSISTENT_TEXT_PLAIN,
                    message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }

        channel.close();
        connection.close();
    }


    public static String getMessage(String[] strings) {
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


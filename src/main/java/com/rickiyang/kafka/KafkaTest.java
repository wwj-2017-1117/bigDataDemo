package com.rickiyang.kafka;

import com.rickiyang.avro.User;

import java.util.List;
import java.util.Scanner;

/**
 * @Author yangyue
 * @Date Created in 下午4:18 2018/11/23
 * @Modified by:
 * @Description:
 **/
public class KafkaTest {

    public static void main(String[] args) {
        Producer<User> producer = new Producer<>();
        Consumer<User> consumer = new Consumer<>();

        System.out.println("Please input 'send', 'receive', or 'exit'");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String input = scanner.next();

            switch (input) {
                case "send":
                    producer.sendData(Topic.USER, new User("xiaoming", 12, 1));
                    break;
                case "receive":
                    List<User> users = consumer.receive(Topic.USER);
                    if (users.isEmpty()) {
                        System.out.println("Received nothing");
                    } else {
                        users.forEach(user -> System.out.println("Received user: " + user));
                    }
                    break;
                case "exit":
                    System.exit(0);
                    break;
                default:
                    System.out.println("Please input 'send', 'receive', or 'exit'");
            }
        }
    }
}

package com.robertocarneiro.kafkaproducerapi.util;

import java.util.UUID;

public class Util {

    private Util() {}

    public static String generateKey(String topic) {
        return topic + "-" + UUID.randomUUID().toString();
    }

}

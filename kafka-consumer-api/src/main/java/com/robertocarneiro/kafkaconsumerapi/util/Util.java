package com.robertocarneiro.kafkaconsumerapi.util;

public class Util {

    private Util() {}

    public static final String LOG_MESSAGE_EXCEPTION = "Testando exception para enviar para DLT";

    public static String logConsumer(Long offset, Object key, Object payload) {
        return "\n\t- Offset: " + offset + "\n\t- Key: " + key + "\n\t- Payload: " + payload;
    }

    public static String logException(String topic, String topicDlt) {
        return "Testando exception no tópico='" + topic + "' para enviar para DLT='" + topicDlt + "'";
    }
}

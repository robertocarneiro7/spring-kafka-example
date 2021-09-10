package com.robertocarneiro.kafkaconsumerapi.util;

public class Util {

    private Util() {}

    public static String LOG_MESSAGE_EXCEPTION = "Testando exception para demonstrar que t�pico n�o commita(n�o atualiza o offset)";

    public static String logConsumer(Long offset, Object key, Object payload) {
        return "\n\t- Offset: " + offset + "\n\t- Key: " + key + "\n\t- Payload: " + payload;
    }
}

package com.simplekafka.client;

public interface MessageHandler {
    void handle(byte[] message, long offset);
}

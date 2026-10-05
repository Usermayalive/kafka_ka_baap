package com.simplekafka.network;

import java.io.IOException;

public class BrokerOverloadedException extends IOException {
    public BrokerOverloadedException(String message) {
        super(message);
    }
}

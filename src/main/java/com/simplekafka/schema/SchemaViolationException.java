package com.simplekafka.schema;

/** A produced record doesn't satisfy its topic's schema; the message says why. */
public class SchemaViolationException extends Exception {
    public SchemaViolationException(String message) {
        super(message);
    }
}

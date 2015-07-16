package io.miguel0afd.cassandra.data.filler.exceptions;

public class CassandraException extends RuntimeException{

    public CassandraException(String message) {
        super(message);
    }
}

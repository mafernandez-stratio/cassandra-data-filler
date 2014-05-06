package com.rojocarmesi.cassandra.exceptions;

public class CassandraException extends RuntimeException{

    public CassandraException(String message) {
        super(message);
    }
}

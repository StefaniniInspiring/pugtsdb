package com.inspiring.pugtsdb.sql;

public class PugSQLException extends RuntimeException {

    public PugSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public PugSQLException(String message, Object... args) {
        this(String.format(message, args), (Throwable) args[args.length - 1]);
    }
}

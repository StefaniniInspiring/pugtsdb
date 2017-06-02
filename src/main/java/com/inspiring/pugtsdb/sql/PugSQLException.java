package com.inspiring.pugtsdb.sql;

import com.inspiring.pugtsdb.exception.PugException;

public class PugSQLException extends PugException {

    public PugSQLException(String message, Throwable cause) {
        super(message, cause);
    }

    public PugSQLException(String message, Object... args) {
        this(String.format(message, args), (Throwable) args[args.length - 1]);
    }
}

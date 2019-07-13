package edu.ubb.dissertation.sensordatahandler.exception;

public class ConnectionException extends RuntimeException {

    public ConnectionException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public static ConnectionException create(final Throwable cause) {
        return new ConnectionException(cause.getMessage(), cause);
    }
}

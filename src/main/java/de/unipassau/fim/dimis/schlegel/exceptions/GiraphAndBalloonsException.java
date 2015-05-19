package de.unipassau.fim.dimis.schlegel.exceptions;

/**
 * A runtime exception used throughout the giraph and balloons project.
 */
public class GiraphAndBalloonsException extends RuntimeException {

    /**
     * Creates a new exception with the error message given.
     *
     * @param message the error message
     */
    public GiraphAndBalloonsException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with the error message and the inner exception given.
     *
     * @param message the error message
     * @param cause   any inner exception or cause
     */
    public GiraphAndBalloonsException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new exception with the inner exception given.
     *
     * @param cause any inner exception or cause
     */
    public GiraphAndBalloonsException(Throwable cause) {
        super(cause);
    }
}

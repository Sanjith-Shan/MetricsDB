package io.metricsdb.promql;

/** A query that does not parse, or uses something outside the supported subset. */
public final class ParseException extends RuntimeException {
    public ParseException(String message, int pos) {
        super(message + (pos >= 0 ? " at position " + pos : ""));
    }
}

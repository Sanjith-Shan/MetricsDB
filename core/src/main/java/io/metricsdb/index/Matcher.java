package io.metricsdb.index;

import io.metricsdb.model.Labels;

import java.util.regex.Pattern;

/** One label matcher of a series selector: {@code name="v"}, {@code !=}, {@code =~} or {@code !~}. */
public final class Matcher {
    public enum Type { EQ, NEQ, RE, NRE }

    public final Type type;
    public final String name;
    public final String value;
    private final Pattern pattern;

    public Matcher(Type type, String name, String value) {
        this.type = type;
        this.name = name;
        this.value = value;
        this.pattern = (type == Type.RE || type == Type.NRE) ? Pattern.compile(value, Pattern.DOTALL) : null;
    }

    public static Matcher eq(String name, String value) { return new Matcher(Type.EQ, name, value); }
    public static Matcher re(String name, String value) { return new Matcher(Type.RE, name, value); }

    /** True when the matcher accepts the given label value; a missing label has value "". */
    public boolean matches(String v) {
        if (v == null) v = "";
        return switch (type) {
            case EQ -> value.equals(v);
            case NEQ -> !value.equals(v);
            case RE -> pattern.matcher(v).matches();
            case NRE -> !pattern.matcher(v).matches();
        };
    }

    public boolean matches(Labels l) { return matches(l.get(name)); }

    /** Positive matchers select by value; negative ones (and anything matching "") subtract. */
    public boolean matchesEmpty() { return matches(""); }

    public boolean isPositive() { return type == Type.EQ || type == Type.RE; }

    @Override public String toString() {
        String op = switch (type) { case EQ -> "="; case NEQ -> "!="; case RE -> "=~"; case NRE -> "!~"; };
        return name + op + '"' + value + '"';
    }
}

package io.metricsdb.promql;

import io.metricsdb.index.Matcher;

import java.util.List;

/** Syntax tree of the supported PromQL subset. */
public final class Ast {
    private Ast() {}

    public sealed interface Expr permits Num, Str, VectorSel, MatrixSel, Call, Agg, Binary, Neg {}

    public record Num(double value) implements Expr {}

    public record Str(String value) implements Expr {}

    public record VectorSel(String name, List<Matcher> matchers, long offsetMs) implements Expr {}

    public record MatrixSel(VectorSel sel, long rangeMs) implements Expr {}

    public record Call(String func, List<Expr> args) implements Expr {}

    public record Agg(String op, List<String> grouping, boolean without, Expr param, Expr expr) implements Expr {}

    public record Binary(String op, Expr lhs, Expr rhs, boolean returnBool) implements Expr {}

    public record Neg(Expr expr) implements Expr {}
}

package io.metricsdb.promql;

import io.metricsdb.index.Matcher;
import io.metricsdb.model.Labels;
import io.metricsdb.promql.Lexer.T;
import io.metricsdb.promql.Lexer.Tok;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Recursive-descent parser for the supported subset: selectors with all four matcher types,
 * range selectors, {@code offset}, function calls, aggregations with {@code by}/{@code without}
 * on either side, and arithmetic and comparison operators with PromQL precedence.
 */
public final class Parser {
    static final Set<String> AGGREGATIONS = Set.of("sum", "avg", "min", "max", "count", "stddev", "stdvar",
            "topk", "bottomk", "quantile", "group");

    private final List<Tok> toks;
    private int p;

    private Parser(String q) {
        this.toks = Lexer.lex(q);
    }

    public static Ast.Expr parse(String q) {
        Parser ps = new Parser(q);
        Ast.Expr e = ps.expr();
        if (ps.peek().type() != T.EOF) throw new ParseException("unexpected " + ps.peek().text(), ps.peek().pos());
        return e;
    }

    private Tok peek() { return toks.get(p); }
    private Tok next() { return toks.get(p++); }

    private Tok expect(T t) {
        Tok k = next();
        if (k.type() != t) throw new ParseException("expected " + t + " but found '" + k.text() + "'", k.pos());
        return k;
    }

    private boolean accept(T t) {
        if (peek().type() == t) { p++; return true; }
        return false;
    }

    private Ast.Expr expr() { return comparison(); }

    private Ast.Expr comparison() {
        Ast.Expr lhs = additive();
        while (true) {
            T t = peek().type();
            if (t != T.EQLC && t != T.NEQ && t != T.GTR && t != T.LSS && t != T.GTE && t != T.LTE) return lhs;
            String op = next().text();
            boolean bool = false;
            if (peek().type() == T.IDENT && peek().text().equals("bool")) { p++; bool = true; }
            lhs = new Ast.Binary(op, lhs, additive(), bool);
        }
    }

    private Ast.Expr additive() {
        Ast.Expr lhs = multiplicative();
        while (peek().type() == T.ADD || peek().type() == T.SUB) {
            String op = next().text();
            lhs = new Ast.Binary(op, lhs, multiplicative(), false);
        }
        return lhs;
    }

    private Ast.Expr multiplicative() {
        Ast.Expr lhs = power();
        while (peek().type() == T.MUL || peek().type() == T.DIV || peek().type() == T.MOD) {
            String op = next().text();
            lhs = new Ast.Binary(op, lhs, power(), false);
        }
        return lhs;
    }

    private Ast.Expr power() {
        Ast.Expr base = unary();
        if (peek().type() == T.POW) {
            p++;
            return new Ast.Binary("^", base, power(), false); // right associative
        }
        return base;
    }

    private Ast.Expr unary() {
        if (accept(T.SUB)) {
            Ast.Expr e = unary();
            return e instanceof Ast.Num n ? new Ast.Num(-n.value()) : new Ast.Neg(e);
        }
        if (accept(T.ADD)) return unary();
        return postfix(primary());
    }

    private Ast.Expr postfix(Ast.Expr e) {
        while (true) {
            if (peek().type() == T.LBRACK) {
                if (!(e instanceof Ast.VectorSel vs)) throw new ParseException("range is only allowed on a selector", peek().pos());
                p++;
                Tok d = expect(T.DURATION);
                expect(T.RBRACK);
                e = new Ast.MatrixSel(vs, Lexer.parseDuration(d.text(), d.pos()));
            } else if (peek().type() == T.IDENT && peek().text().equals("offset")) {
                p++;
                Tok d = expect(T.DURATION);
                long off = Lexer.parseDuration(d.text(), d.pos());
                if (e instanceof Ast.VectorSel vs) e = new Ast.VectorSel(vs.name(), vs.matchers(), off);
                else if (e instanceof Ast.MatrixSel ms) e = new Ast.MatrixSel(new Ast.VectorSel(ms.sel().name(), ms.sel().matchers(), off), ms.rangeMs());
                else throw new ParseException("offset is only allowed on a selector", d.pos());
            } else {
                return e;
            }
        }
    }

    private Ast.Expr primary() {
        Tok t = peek();
        switch (t.type()) {
            case NUMBER -> {
                p++;
                String s = t.text();
                double v = s.startsWith("0x") || s.startsWith("0X") ? Long.parseLong(s.substring(2), 16) : Double.parseDouble(s);
                return new Ast.Num(v);
            }
            case STRING -> { p++; return new Ast.Str(t.text()); }
            case LPAREN -> {
                p++;
                Ast.Expr e = expr();
                expect(T.RPAREN);
                return e;
            }
            case LBRACE -> { return selector(null); }
            case IDENT -> {
                String id = t.text();
                if (id.equalsIgnoreCase("inf")) { p++; return new Ast.Num(Double.POSITIVE_INFINITY); }
                if (id.equalsIgnoreCase("nan")) { p++; return new Ast.Num(Double.NaN); }
                Tok after = toks.get(p + 1);
                if (AGGREGATIONS.contains(id) && (after.type() == T.LPAREN
                        || (after.type() == T.IDENT && (after.text().equals("by") || after.text().equals("without"))))) {
                    return aggregation();
                }
                if (after.type() == T.LPAREN) return call();
                p++;
                return selector(id);
            }
            default -> throw new ParseException("unexpected '" + t.text() + "'", t.pos());
        }
    }

    private Ast.Expr selector(String name) {
        List<Matcher> ms = new ArrayList<>();
        if (name != null) ms.add(Matcher.eq(Labels.NAME, name));
        if (accept(T.LBRACE)) {
            while (peek().type() != T.RBRACE) {
                Tok ln = next();
                if (ln.type() != T.IDENT && ln.type() != T.STRING) throw new ParseException("expected label name", ln.pos());
                Tok op = next();
                Matcher.Type mt = switch (op.type()) {
                    case EQ -> Matcher.Type.EQ;
                    case NEQ -> Matcher.Type.NEQ;
                    case RE -> Matcher.Type.RE;
                    case NRE -> Matcher.Type.NRE;
                    default -> throw new ParseException("expected matcher operator", op.pos());
                };
                Tok val = expect(T.STRING);
                ms.add(new Matcher(mt, ln.text(), val.text()));
                if (!accept(T.COMMA)) break;
            }
            expect(T.RBRACE);
        }
        if (ms.isEmpty()) throw new ParseException("a selector needs a metric name or a matcher", peek().pos());
        boolean anyNonEmpty = ms.stream().anyMatch(m -> !m.matchesEmpty());
        if (!anyNonEmpty) throw new ParseException("selector must contain at least one matcher that does not match the empty string", peek().pos());
        return new Ast.VectorSel(name, ms, 0);
    }

    private Ast.Expr call() {
        Tok f = next();
        expect(T.LPAREN);
        List<Ast.Expr> args = new ArrayList<>();
        if (peek().type() != T.RPAREN) {
            do { args.add(expr()); } while (accept(T.COMMA));
        }
        expect(T.RPAREN);
        if (!Functions.isKnown(f.text())) throw new ParseException("unsupported function " + f.text(), f.pos());
        return new Ast.Call(f.text(), args);
    }

    private Ast.Expr aggregation() {
        String op = next().text();
        List<String> grouping = null;
        boolean without = false;
        if (peek().type() == T.IDENT && (peek().text().equals("by") || peek().text().equals("without"))) {
            without = next().text().equals("without");
            grouping = labelList();
        }
        expect(T.LPAREN);
        Ast.Expr param = null;
        Ast.Expr e = expr();
        if (accept(T.COMMA)) {
            param = e;
            e = expr();
        }
        expect(T.RPAREN);
        if (grouping == null && peek().type() == T.IDENT && (peek().text().equals("by") || peek().text().equals("without"))) {
            without = next().text().equals("without");
            grouping = labelList();
        }
        boolean needsParam = op.equals("topk") || op.equals("bottomk") || op.equals("quantile");
        if (needsParam != (param != null)) throw new ParseException(op + (needsParam ? " needs" : " takes no") + " parameter", peek().pos());
        return new Ast.Agg(op, grouping == null ? List.of() : grouping, without, param, e);
    }

    private List<String> labelList() {
        expect(T.LPAREN);
        List<String> out = new ArrayList<>();
        while (peek().type() != T.RPAREN) {
            Tok l = next();
            if (l.type() != T.IDENT) throw new ParseException("expected label name", l.pos());
            out.add(l.text());
            if (!accept(T.COMMA)) break;
        }
        expect(T.RPAREN);
        return out;
    }
}

package io.metricsdb.promql;

import java.util.ArrayList;
import java.util.List;

/** Tokenizer for the supported PromQL subset. */
final class Lexer {
    enum T { IDENT, NUMBER, DURATION, STRING, LBRACE, RBRACE, LPAREN, RPAREN, LBRACK, RBRACK, COMMA,
        EQ, NEQ, RE, NRE, ADD, SUB, MUL, DIV, MOD, POW, EQLC, GTR, LSS, GTE, LTE, EOF }

    record Tok(T type, String text, int pos) {
        @Override public String toString() { return type + "(" + text + ")@" + pos; }
    }

    static List<Tok> lex(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0, n = s.length();
        boolean inBracket = false;
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            if (c == '#') { while (i < n && s.charAt(i) != '\n') i++; continue; }
            int start = i;
            switch (c) {
                case '{' -> { out.add(new Tok(T.LBRACE, "{", i)); i++; continue; }
                case '}' -> { out.add(new Tok(T.RBRACE, "}", i)); i++; continue; }
                case '(' -> { out.add(new Tok(T.LPAREN, "(", i)); i++; continue; }
                case ')' -> { out.add(new Tok(T.RPAREN, ")", i)); i++; continue; }
                case '[' -> { out.add(new Tok(T.LBRACK, "[", i)); i++; inBracket = true; continue; }
                case ']' -> { out.add(new Tok(T.RBRACK, "]", i)); i++; inBracket = false; continue; }
                case ',' -> { out.add(new Tok(T.COMMA, ",", i)); i++; continue; }
                case '+' -> { out.add(new Tok(T.ADD, "+", i)); i++; continue; }
                case '*' -> { out.add(new Tok(T.MUL, "*", i)); i++; continue; }
                case '/' -> { out.add(new Tok(T.DIV, "/", i)); i++; continue; }
                case '%' -> { out.add(new Tok(T.MOD, "%", i)); i++; continue; }
                case '^' -> { out.add(new Tok(T.POW, "^", i)); i++; continue; }
                case '-' -> { out.add(new Tok(T.SUB, "-", i)); i++; continue; }
                case '=' -> {
                    if (i + 1 < n && s.charAt(i + 1) == '=') { out.add(new Tok(T.EQLC, "==", i)); i += 2; }
                    else if (i + 1 < n && s.charAt(i + 1) == '~') { out.add(new Tok(T.RE, "=~", i)); i += 2; }
                    else { out.add(new Tok(T.EQ, "=", i)); i++; }
                    continue;
                }
                case '!' -> {
                    if (i + 1 < n && s.charAt(i + 1) == '=') { out.add(new Tok(T.NEQ, "!=", i)); i += 2; }
                    else if (i + 1 < n && s.charAt(i + 1) == '~') { out.add(new Tok(T.NRE, "!~", i)); i += 2; }
                    else throw new ParseException("unexpected '!'", i);
                    continue;
                }
                case '>' -> {
                    if (i + 1 < n && s.charAt(i + 1) == '=') { out.add(new Tok(T.GTE, ">=", i)); i += 2; }
                    else { out.add(new Tok(T.GTR, ">", i)); i++; }
                    continue;
                }
                case '<' -> {
                    if (i + 1 < n && s.charAt(i + 1) == '=') { out.add(new Tok(T.LTE, "<=", i)); i += 2; }
                    else { out.add(new Tok(T.LSS, "<", i)); i++; }
                    continue;
                }
                case '"', '\'', '`' -> {
                    StringBuilder sb = new StringBuilder();
                    i++;
                    while (i < n && s.charAt(i) != c) {
                        char d = s.charAt(i);
                        if (d == '\\' && c != '`' && i + 1 < n) {
                            char e = s.charAt(++i);
                            switch (e) {
                                case 'n' -> sb.append('\n');
                                case 't' -> sb.append('\t');
                                case '\\' -> sb.append('\\');
                                case '"' -> sb.append('"');
                                case '\'' -> sb.append('\'');
                                default -> sb.append('\\').append(e); // keep regex escapes like \. intact
                            }
                        } else {
                            sb.append(d);
                        }
                        i++;
                    }
                    if (i >= n) throw new ParseException("unterminated string", start);
                    i++;
                    out.add(new Tok(T.STRING, sb.toString(), start));
                    continue;
                }
                default -> { }
            }
            if (Character.isDigit(c) || (c == '.' && i + 1 < n && Character.isDigit(s.charAt(i + 1)))) {
                // a duration is digits followed by a unit letter; otherwise a number
                int j = i;
                while (j < n && Character.isDigit(s.charAt(j))) j++;
                if (j < n && isUnitStart(s, j) && !(j + 1 < n && s.charAt(j) == 'e' && Character.isDigit(s.charAt(j + 1)))) {
                    int k = i;
                    while (k < n && (Character.isLetterOrDigit(s.charAt(k)))) k++;
                    out.add(new Tok(T.DURATION, s.substring(i, k), i));
                    i = k;
                    continue;
                }
                int k = i;
                if (s.startsWith("0x", k) || s.startsWith("0X", k)) {
                    k += 2;
                    while (k < n && Character.digit(s.charAt(k), 16) >= 0) k++;
                } else {
                    while (k < n && (Character.isDigit(s.charAt(k)) || s.charAt(k) == '.')) k++;
                    if (k < n && (s.charAt(k) == 'e' || s.charAt(k) == 'E')) {
                        k++;
                        if (k < n && (s.charAt(k) == '+' || s.charAt(k) == '-')) k++;
                        while (k < n && Character.isDigit(s.charAt(k))) k++;
                    }
                }
                out.add(new Tok(T.NUMBER, s.substring(i, k), i));
                i = k;
                continue;
            }
            if (Character.isLetter(c) || c == '_' || c == ':') {
                int k = i;
                while (k < n && (Character.isLetterOrDigit(s.charAt(k)) || s.charAt(k) == '_' || s.charAt(k) == ':')) k++;
                out.add(new Tok(T.IDENT, s.substring(i, k), i));
                i = k;
                continue;
            }
            throw new ParseException("unexpected character '" + c + "'", i);
        }
        out.add(new Tok(T.EOF, "", n));
        return out;
    }

    private static boolean isUnitStart(String s, int j) {
        char u = s.charAt(j);
        return u == 's' || u == 'm' || u == 'h' || u == 'd' || u == 'w' || u == 'y';
    }

    static long parseDuration(String d, int pos) {
        long total = 0;
        int i = 0;
        boolean any = false;
        while (i < d.length()) {
            int j = i;
            while (j < d.length() && Character.isDigit(d.charAt(j))) j++;
            if (j == i) throw new ParseException("bad duration " + d, pos);
            long num = Long.parseLong(d.substring(i, j));
            int k = j;
            while (k < d.length() && Character.isLetter(d.charAt(k))) k++;
            String unit = d.substring(j, k);
            long ms = switch (unit) {
                case "ms" -> 1L;
                case "s" -> 1_000L;
                case "m" -> 60_000L;
                case "h" -> 3_600_000L;
                case "d" -> 86_400_000L;
                case "w" -> 7 * 86_400_000L;
                case "y" -> 365 * 86_400_000L;
                default -> throw new ParseException("bad duration unit '" + unit + "'", pos);
            };
            total += num * ms;
            any = true;
            i = k;
        }
        if (!any) throw new ParseException("empty duration", pos);
        return total;
    }
}

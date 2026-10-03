package io.metricsdb.bench;

import java.util.Arrays;

/** Entry point for the experiment tools: {@code bench <command> [--flag value ...]}. */
public final class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("commands: load, querybench, diff, exp5, ooo, faults");
            System.exit(2);
        }
        Args a = new Args(Arrays.copyOfRange(args, 1, args.length));
        switch (args[0]) {
            case "load" -> Load.run(a);
            case "querybench" -> QueryBench.run(a);
            case "diff" -> Diff.run(a);
            case "exp5" -> Exp5.run(a);
            case "ooo" -> OooExp.run(a);
            default -> throw new IllegalArgumentException("unknown command " + args[0]);
        }
    }
}

package io.metricsdb.storage;

import io.metricsdb.index.Matcher;

import java.util.List;

/**
 * What the query engine needs from storage. A single node implements it over its own data; the
 * query router implements it by fanning out to the storage nodes and merging replicas.
 */
public interface Queryable {
    List<SeriesChunks> select(List<Matcher> matchers, long mint, long maxt, QueryContext ctx);

    /** Rollup chunks at {@code res}, or null when this storage cannot serve that resolution. */
    List<RollupSeries> selectRollup(List<Matcher> matchers, long mint, long maxt, long res, QueryContext ctx);
}

package io.metricsdb.storage;

import io.metricsdb.model.Labels;

import java.util.List;

/** Downsampled chunks of one series: {@code aggs[k]} holds the chunks of aggregate k (see {@code Rollup}). */
public record RollupSeries(Labels labels, List<Chunk>[] aggs) {}

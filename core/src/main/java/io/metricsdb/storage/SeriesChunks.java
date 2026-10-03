package io.metricsdb.storage;

import io.metricsdb.model.Labels;

import java.util.List;

/** The selected chunks of one series, in no particular order; they may overlap. */
public record SeriesChunks(Labels labels, List<Chunk> chunks) {}

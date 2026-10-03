package io.metricsdb.server;

import io.metricsdb.storage.WriteBatch;

/** Where parsed writes go: the local database, or the router's replicated write path. */
public interface Ingester {

    /**
     * @param accepted        samples stored (durable on enough replicas)
     * @param overCardinality samples refused because a new series would exceed the series limit
     * @param tooOld          samples refused as older than the out-of-order window allows
     * @param unavailable     samples that could not reach enough replicas; the client should retry
     */
    record Result(long accepted, long overCardinality, long tooOld, long unavailable, String detail) {
        public static Result merge(Result a, Result b) {
            return new Result(a.accepted + b.accepted, a.overCardinality + b.overCardinality, a.tooOld + b.tooOld,
                    a.unavailable + b.unavailable, a.detail != null ? a.detail : b.detail);
        }
    }

    Result write(WriteBatch batch);
}

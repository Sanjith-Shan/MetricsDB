package io.metricsdb.server;

import io.metricsdb.storage.Tsdb;
import io.metricsdb.storage.WriteBatch;

/** Writes straight into this node's database. */
public class LocalIngester implements Ingester {
    private final Tsdb db;
    private final String node;

    public LocalIngester(Tsdb db, String node) {
        this.db = db;
        this.node = node;
    }

    @Override
    public Result write(WriteBatch batch) {
        Tsdb.AppendResult r = db.append(batch);
        String detail = null;
        if (r.overCardinality() > 0) {
            detail = "cardinality limit reached on " + node + ": the node holds " + db.seriesCount()
                    + " series (limit " + db.options().maxSeries + ", metricsdb.storage.max-series); "
                    + r.overCardinality() + " samples for new series were refused";
        } else if (r.tooOld() > 0) {
            detail = r.tooOld() + " samples were older than the out-of-order window allows and were refused";
        }
        return new Result(r.accepted() + r.duplicates(), r.overCardinality(), r.tooOld(), 0, detail);
    }
}

package org.apache.cassandra.service;

import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.tracing.Tracing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RepairedQuorumReadRowResolver extends AbstractRowResolver
{

    private final ReadCommand cmd;
    private final ColumnFamilyStore cfs;

    public RepairedQuorumReadRowResolver(ReadCommand cmd, ColumnFamilyStore cfs)
    {
        super(cmd.key, cmd.ksName);
        this.cmd = cmd;
        this.cfs = cfs;
    }

    /*
    * This method merges data read from the local coordinator with
    * since-repairedAt data from replicas during a REPAIRED_QUORUM read
    */
    public Row resolve() throws DigestMismatchException
    {
        if (logger.isDebugEnabled())
            logger.debug("merging {} repaired quorum responses", replies.size());

        long start = System.nanoTime();

        List<Iterator<Cell>> iterators = new ArrayList<>();
        ColumnFamily mergedCf = ArrayBackedSortedColumns.factory.create(cfs.metadata, cmd.filter().isReversed());

        for (MessageIn<ReadResponse> message : replies)
        {
            ReadResponse response = message.payload;
            ColumnFamily cf = response.row().cf;
            if (cf != null) {
                iterators.add(cf.iterator());
                mergedCf.delete(cf);
            }
        }

        DecoratedKey dk = StorageService.getPartitioner().decorateKey(cmd.key);
        QueryFilter filter = new QueryFilter(dk, cmd.cfName, cmd.filter(), cmd.timestamp);

        Tracing.trace("Merged {} responses during REPAIRED_QUORUM read.", new Object[]{replies.size()});

        filter.collateColumns(mergedCf, iterators, cfs.gcBefore(cmd.timestamp));

        if (logger.isDebugEnabled())
            logger.debug("repaired quorum merge: {} ms.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        return new Row(key, mergedCf);

    }

    public boolean isDataPresent()
    {
        return !replies.isEmpty();
    }

    public Row getData()
    {
        try {
            return resolve();
        } catch (DigestMismatchException e) {
            throw new RuntimeException(e); //will never be thrown
        }
    }
}

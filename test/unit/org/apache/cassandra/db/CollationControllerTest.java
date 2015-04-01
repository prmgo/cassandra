/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import org.apache.cassandra.db.compaction.AntiCompactionTest;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ActiveRepairService;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class CollationControllerTest extends SchemaLoader
{
    @Test
    public void getTopLevelColumnsSkipsSSTablesModifiedBeforeRowDelete() 
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");
        
        // add data
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.add(cfs.name, Util.cellname("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        cfs.forceBlockingFlush();
        
        // remove
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.delete(cfs.name, 10);
        rm.apply();
        
        // add another mutation because sstable maxtimestamp isn't set
        // correctly during flush if the most recent mutation is a row delete
        rm = new Mutation(keyspace.getName(), Util.dk("key2").getKey());
        rm.add(cfs.name, Util.cellname("Column1"), ByteBufferUtil.bytes("zxcv"), 20);
        rm.apply();
        
        cfs.forceBlockingFlush();

        // add yet one more mutation
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.add(cfs.name, Util.cellname("Column1"), ByteBufferUtil.bytes("foobar"), 30);
        rm.apply();
        cfs.forceBlockingFlush();

        // A NamesQueryFilter goes down one code path (through collectTimeOrderedData())
        // It should only iterate the last flushed sstable, since it probably contains the most recent value for Column1
        QueryFilter filter = Util.namesQueryFilter(cfs, dk, "Column1");
        CollationController controller = new CollationController(cfs, filter, Integer.MIN_VALUE, ActiveRepairService.UNREPAIRED_SSTABLE);
        controller.getTopLevelColumns(true);
        assertEquals(1, controller.getSstablesIterated());

        // SliceQueryFilter goes down another path (through collectAllData())
        // We will read "only" the last sstable in that case, but because the 2nd sstable has a tombstone that is more
        // recent than the maxTimestamp of the very first sstable we flushed, we should only read the 2 first sstables.
        filter = QueryFilter.getIdentityFilter(dk, cfs.name, System.currentTimeMillis());
        controller = new CollationController(cfs, filter, Integer.MIN_VALUE, ActiveRepairService.UNREPAIRED_SSTABLE);
        controller.getTopLevelColumns(true);
        assertEquals(2, controller.getSstablesIterated());
    }

    @Test
    public void getTopLevelColumnsSkipsSSTablesRepairedOnOrBeforeMaxRepairedAt() throws Exception
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        //creates an sstable with keys from 0 to 9 (cols 1-10)
        AntiCompactionTest.prepareColumnFamilyStore();

        //repair keys from 0 to 4
        Range<Token> range = new Range<Token>(new BytesToken("0".getBytes()), new BytesToken("4".getBytes()));
        AntiCompactionTest.performAntiCompactionOnRange(cfs, range, 100);

        // We should have 2 sstables:
        // repaired: keys(0,4) maxRepairedAt=100
        // unrepaired: keys(5,9) maxRepairedAt=0
        assertEquals(2, cfs.getSSTables().size());

        // create new unrepaired SSTable with entries (Key/Column) (2,2), (6,6) and (9,9)
        mutateKey("2", keyspace, cfs);
        mutateKey("6", keyspace, cfs);
        mutateKey("9", keyspace, cfs);
        cfs.forceBlockingFlush();

        // We should have 3 sstables:
        // repaired: keys(0,4) maxRepairedAt=100
        // unrepaired: keys(5,9) maxRepairedAt=0
        // unrepaired: keys[2, 6, 9] maxRepairedAt=0
        assertEquals(3, cfs.getSSTables().size());

        /* collectTimeOrderedData() path */

        //let's first try to fetch key/column (2,3), without the maxRepairAt optimization: should iterate over 2 sstables
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "2", "3", ActiveRepairService.UNREPAIRED_SSTABLE, 2);
        //let's first try to fetch key/column (2,3), with maxRepairAt=99: should iterate over 2 sstables (none is ignored)
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "2", "3", 99, 2);
        //let's first try to fetch key/column (2,3), with maxRepairAt=100: should iterate over 1 sstable
        // (1 is ignored since repairedAt=100 <= 100)
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "2", "3", 100, 1);
        //let's first try to fetch key/column (2,3), with maxRepairAt=101: should iterate over 1 sstable
        // (1 is ignored since repairedAt=100 <= 101)
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "2", "3", 101, 1);
        //if we try to fetch key/column (6,3), it doesn't matter if we use the optimization or not, because this
        //range was never repaired
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "6", "3", ActiveRepairService.UNREPAIRED_SSTABLE, 2);
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "6", "3", 101, 2);

        /* collectAllData() path */

        //let's first try to fetch key=2, without the maxRepairAt optimization: should iterate over 2 sstables
        verifyIteratedSStablesOverCollectAllDataPath(cfs, "2", ActiveRepairService.UNREPAIRED_SSTABLE, 2);
        //let's first try to fetch key=2, with maxRepairAt=99: should iterate over 2 sstables (none is ignored)
        verifyIteratedSStablesOverCollectAllDataPath(cfs, "2", 99, 2);
        //let's first try to fetch key=2, with maxRepairAt=100: should iterate over 1 sstable
        // (1 is ignored since repairedAt=100 <= 100)
        verifyIteratedSStablesOverCollectAllDataPath(cfs, "2", 100, 1);
        //let's first try to fetch key=2, with maxRepairAt=101: should iterate over 1 sstable
        // (1 is ignored since repairedAt=100 <= 101)
        verifyIteratedSStablesOverCollectAllDataPath(cfs, "2", 101, 1);
        //if we try to fetch key=6, it doesn't matter if we use the optimization or not, because this
        //range was never repaired
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "6", "3", ActiveRepairService.UNREPAIRED_SSTABLE, 2);
        verifyIteratedSStablesOverCollectTimeOrderedDataPath(cfs, "6", "3", 101, 2);
    }

    private void verifyIteratedSStablesOverCollectTimeOrderedDataPath(ColumnFamilyStore cfs, String key, String col,
                                                                      long maxRepairedAt, int expectedSStablesIterated) {
        // A NamesQueryFilter goes down one code path (through collectTimeOrderedData())
        DecoratedKey dk = Util.dk(key);
        QueryFilter filter = Util.namesQueryFilter(cfs, dk, Util.cellname(col));
        CollationController controller = new CollationController(cfs, filter, Integer.MIN_VALUE, maxRepairedAt);
        controller.getTopLevelColumns(true);
        assertEquals(expectedSStablesIterated, controller.getSstablesIterated());
    }

    private void verifyIteratedSStablesOverCollectAllDataPath(ColumnFamilyStore cfs, String key, long maxRepairedAt,
                                                              int expectedSStablesIterated) {
        // SliceQueryFilter goes down another path (through collectAllData())
        DecoratedKey dk = Util.dk(key);
        QueryFilter filter = QueryFilter.getIdentityFilter(dk, cfs.name, System.currentTimeMillis());
        CollationController controller = new CollationController(cfs, filter, Integer.MIN_VALUE, maxRepairedAt);
        controller.getTopLevelColumns(true);
        assertEquals(expectedSStablesIterated, controller.getSstablesIterated());
    }

    private void mutateKey(String key, Keyspace keyspace, ColumnFamilyStore cfs) {
        DecoratedKey dk = Util.dk(key);
        Mutation rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.add(cfs.name, Util.cellname(key), ByteBufferUtil.bytes("asdf"), System.currentTimeMillis(), 0);
        rm.apply();
    }

    @Test
    public void ensureTombstonesAppliedAfterGCGS()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("StandardGCGS0");
        cfs.disableAutoCompaction();

        Mutation rm;
        DecoratedKey dk = Util.dk("key1");
        CellName cellName = Util.cellname("Column1");

        // add data
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.add(cfs.name, cellName, ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        cfs.forceBlockingFlush();

        // remove
        rm = new Mutation(keyspace.getName(), dk.getKey());
        rm.delete(cfs.name, cellName, 0);
        rm.apply();
        cfs.forceBlockingFlush();

        // use "realistic" query times since we'll compare these numbers to the local deletion time of the tombstone
        QueryFilter filter;
        long queryAt = System.currentTimeMillis() + 1000;
        int gcBefore = cfs.gcBefore(queryAt);

        filter = QueryFilter.getNamesFilter(dk, cfs.name, FBUtilities.singleton(cellName, cfs.getComparator()), queryAt);
        CollationController controller = new CollationController(cfs, filter, gcBefore, ActiveRepairService.UNREPAIRED_SSTABLE);
        assert ColumnFamilyStore.removeDeleted(controller.getTopLevelColumns(true), gcBefore) == null;

        filter = QueryFilter.getIdentityFilter(dk, cfs.name, queryAt);
        controller = new CollationController(cfs, filter, gcBefore, ActiveRepairService.UNREPAIRED_SSTABLE);
        assert ColumnFamilyStore.removeDeleted(controller.getTopLevelColumns(true), gcBefore) == null;
    }
}

package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.repository.rocks.RocksRepositories;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.inspiring.pugtsdb.repository.rocks.RocksRepository.COLUMN_FAMILY_METRIC;
import static com.inspiring.pugtsdb.util.Collections.isEmpty;
import static com.inspiring.pugtsdb.util.Strings.isBlank;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toList;

public class PugTSDBOverRocks extends PugTSDB {

    private static final Logger log = LoggerFactory.getLogger(PugTSDBOverRocks.class);

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final BlockBasedTableConfig blockBasedTableOptions;
    private final Map<String, ColumnFamilyHandle> columnFamilyCache;

    public PugTSDBOverRocks(String storagePath) {
        super(new RocksRepositories());

        if (isBlank(storagePath)) {
            throw new PugIllegalArgumentException("Database storage path cannot be null nor empty");
        }

        this.dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxOpenFiles(999999)
                .setSkipStatsUpdateOnDbOpen(true)
                .setCompactionReadaheadSize(2 * 1024 * 1024)
                .setNewTableReaderForCompactionInputs(true);
        this.columnFamilyOptions = new ColumnFamilyOptions()
                .setOptimizeFiltersForHits(true)
                .setCompactionStyle(CompactionStyle.UNIVERSAL)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setCompactionOptionsUniversal(new CompactionOptionsUniversal()
                                                       .setMaxSizeAmplificationPercent(100)
                                                       .setCompressionSizePercent(-1));
        this.blockBasedTableOptions = new BlockBasedTableConfig()
                .setCacheIndexAndFilterBlocks(true)
                .setBlockSize(256 * 1024);

        try (Options options = new Options(dbOptions, columnFamilyOptions).setTableFormatConfig(blockBasedTableOptions)) {
            log.debug("RocksDB opening...");
            long openStart = currentTimeMillis();

            log.trace("RocksDB loading library...");
            RocksDB.loadLibrary();

            log.trace("RocksDB listing column families...");
            List<byte[]> columnFamiliesNames = RocksDB.listColumnFamilies(options, storagePath);
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = isEmpty(columnFamiliesNames)
                                                                   ? Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                                                                                   new ColumnFamilyDescriptor(COLUMN_FAMILY_METRIC.getBytes(), columnFamilyOptions))
                                                                   : columnFamiliesNames.stream()
                                                                           .map(name -> new ColumnFamilyDescriptor(name, columnFamilyOptions))
                                                                           .collect(toList());
            log.trace("RocksDB opening database...");
            this.db = RocksDB.open(dbOptions, storagePath, columnFamilyDescriptors, columnFamilyHandles);
            this.columnFamilyCache = IntStream.range(0, columnFamilyHandles.size())
                    .boxed()
                    .collect(toConcurrentMap(i -> new String(columnFamilyDescriptors.get(i).columnFamilyName()),
                                             i -> columnFamilyHandles.get(i)));

            log.debug("RocksDB open. Took={}ms", currentTimeMillis() - openStart);
        } catch (RocksDBException e) {
            throw new PugException("Cannot open RocksDB database", e);
        }

        ((RocksRepositories) repositories).setRocksDb(db, columnFamilyOptions, columnFamilyCache);
    }

    @Override
    protected void closeConnection() {
        // nothing to do
    }

    @Override
    protected void commitConnection() {
        // nothing to do
    }

    @Override
    protected void rollbackConnection() {
        // nothing to do
    }

    @Override
    public void close() throws Exception {
        log.trace("Closing scheduled roll ups...");
        rollUpScheduler.close();

        log.trace("RocksDB closing...");
        long closeStart = currentTimeMillis();

        log.trace("RocksDB closing column families handles...");
        columnFamilyCache.values().forEach(ColumnFamilyHandle::close);

        log.trace("RocksDB closing database...");
        db.close();

        log.trace("RocksDB closing database options...");
        dbOptions.close();

        log.trace("RocksDB closing column families options...");
        columnFamilyOptions.close();

        log.debug("RocksDB closed. Took={}ms", currentTimeMillis() - closeStart);
    }
}

package com.inspiring.pugtsdb;

import com.inspiring.pugtsdb.exception.PugException;
import com.inspiring.pugtsdb.exception.PugIllegalArgumentException;
import com.inspiring.pugtsdb.repository.rocks.RocksRepositories;
import java.io.File;
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

import static com.inspiring.pugtsdb.repository.rocks.RocksRepository.METRIC_COLUMN_FAMILY;
import static com.inspiring.pugtsdb.util.Collections.isEmpty;
import static com.inspiring.pugtsdb.util.Strings.isBlank;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toList;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

public class PugTSDBOverRocks extends PugTSDB {

    private static final Logger log = LoggerFactory.getLogger(PugTSDBOverRocks.class);

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions columnFamilyOptions;
    private final BlockBasedTableConfig blockBasedTableOptions;
    private final Map<String, ColumnFamilyHandle> columnFamilyCache;

    public PugTSDBOverRocks(String storagePath) {
        this(storagePath, false);
    }

    public PugTSDBOverRocks(String storagePath, boolean readOnly) {
        super(new RocksRepositories());

        if (isBlank(storagePath)) {
            throw new PugIllegalArgumentException("Database storage path cannot be null nor empty");
        }

        File storageDir = new File(storagePath);

        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }

        this.dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxOpenFiles(-1)
                .setSkipStatsUpdateOnDbOpen(true)
                .setCompactionReadaheadSize(4 * 1024 * 1024)
                .setNewTableReaderForCompactionInputs(true);
        this.columnFamilyOptions = new ColumnFamilyOptions()
                .setOptimizeFiltersForHits(true)
                .setCompactionStyle(CompactionStyle.UNIVERSAL)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setCompactionOptionsUniversal(new CompactionOptionsUniversal()
                                                       .setMaxSizeAmplificationPercent(200)
                                                       .setCompressionSizePercent(-1));
        this.blockBasedTableOptions = new BlockBasedTableConfig()
                .setCacheIndexAndFilterBlocks(true)
                .setBlockSize(256 * 1024);

        try (Options options = new Options(dbOptions, columnFamilyOptions).setTableFormatConfig(blockBasedTableOptions)) {
            log.debug("PugTSDB is opening...");
            long openStart = currentTimeMillis();

            log.trace("RocksDB is loading library...");
            RocksDB.loadLibrary();

            log.trace("RocksDB is listing column families...");
            List<byte[]> columnFamiliesNames = RocksDB.listColumnFamilies(options, storagePath);
            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = isEmpty(columnFamiliesNames)
                                                                   ? Arrays.asList(new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                                                                                   new ColumnFamilyDescriptor(METRIC_COLUMN_FAMILY.getBytes(), columnFamilyOptions))
                                                                   : columnFamiliesNames.stream()
                                                                           .map(name -> new ColumnFamilyDescriptor(name, columnFamilyOptions))
                                                                           .collect(toList());
            log.trace("RocksDB is opening database...");
            this.db = readOnly
                      ? RocksDB.openReadOnly(dbOptions, storagePath, columnFamilyDescriptors, columnFamilyHandles)
                      : RocksDB.open(dbOptions, storagePath, columnFamilyDescriptors, columnFamilyHandles);
            this.columnFamilyCache = IntStream.range(0, columnFamilyHandles.size())
                    .boxed()
                    .collect(toConcurrentMap(i -> new String(columnFamilyDescriptors.get(i).columnFamilyName()),
                                             i -> columnFamilyHandles.get(i)));

            log.debug("PugTSDB open. Took={}ms", currentTimeMillis() - openStart);
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
        log.trace("PugTSDB is closing...");
        long closeStart = currentTimeMillis();

        log.trace("PugTSDB is closing scheduled roll ups...");
        rollUpScheduler.close();

        log.trace("RocksDB is closing column families handles...");
        columnFamilyCache.values().forEach(ColumnFamilyHandle::close);

        log.trace("RocksDB is closing database...");
        db.close();

        log.trace("RocksDB is closing database options...");
        dbOptions.close();

        log.trace("RocksDB is closing column families options...");
        columnFamilyOptions.close();

        log.debug("PugTSDB closed. Took={}ms", currentTimeMillis() - closeStart);
    }

//    public static void main(String[] args) throws Exception {
//        long start = currentTimeMillis();
//
//        try (PugTSDBOverRocks pug = new PugTSDBOverRocks("/tmp/var/db/rocks", true)) {
//            RocksDB db = pug.db;
//
//            pug.columnFamilyCache.keySet().forEach(cf -> System.out.println(cf));
//
//            ColumnFamilyHandle pointRawColumnFamily = pug.columnFamilyCache.get("point:null:null");
//            ColumnFamilyHandle pointCur1mColumnFamily = pug.columnFamilyCache.get("point:cur:1m");
//
//            System.out.println(deserialize(db.get(pug.columnFamilyCache.get("metric"), serialize("os.uptime")), String.class));
//
//            try (ReadOptions options = new ReadOptions().setIgnoreRangeDeletions(true);
//                 RocksIterator iterator = db.newIterator(pointRawColumnFamily, options)) {
//                int t = 0;
//
//                for (iterator.seek(serialize("-0019487545960000000000000")); iterator.isValid(); iterator.next(), t++) {
//                    if (!deserialize(iterator.key(), String.class).startsWith("-001948754596")) {
//                        break;
//                    }
//
//                    System.out.println(deserialize(iterator.key(), String.class) + " = " + deserialize(iterator.value(), Double.class));
//                }
//
//                System.out.println(t);
//            }
//
//            System.out.println(deserialize(db.get(pug.columnFamilyCache.get("metric"), serialize("os.disk")), String.class));
//
//            try (RocksIterator iterator = db.newIterator(pointRawColumnFamily)) {
//                int t = 0;
//
//                for (iterator.seek(serialize("-0015470639460000000000000")); iterator.isValid(); iterator.next(), t++) {
//                    if (!deserialize(iterator.key(), String.class).startsWith("-001547063946")) {
//                        break;
//                    }
//
//                    System.out.println(deserialize(iterator.key(), String.class) + " = " + deserialize(iterator.value(), Double.class));
//                }
//
//                System.out.println(t);
//            }
//        }
//
//        System.out.println("Took " + (currentTimeMillis() - start) + "ms");
//    }
}

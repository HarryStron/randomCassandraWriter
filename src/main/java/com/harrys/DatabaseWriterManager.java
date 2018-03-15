package com.harrys;

import com.datastax.driver.core.Session;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.LoggerFactory;

public class DatabaseWriterManager {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(DatabaseWriterManager.class);

    private final String node;
    private final String username;
    private final String password;
    private final String cluster;
    private final String keyspace;
    private final String table;
    private final List<Column> columns;

    public DatabaseWriterManager(String node, String username, String password, String cluster,
                                 String keyspace, String table, List<Column> columns) {
        this.node = node;
        this.username = username;
        this.password = password;
        this.cluster = cluster;
        this.columns = columns;
        this.keyspace = keyspace;
        this.table = table;
    }

    public void start(int entriesPerRunnable, int concurrencyLevel, int threadLimit) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadLimit);

        for (int i=0; i<concurrencyLevel; i++) {
            executor.execute(new DbWriterRunnable(entriesPerRunnable));
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            Thread.sleep(500);
        }

        logger.info("DONE");
    }

    private class DbWriterRunnable implements Runnable {
        private org.slf4j.Logger logger = LoggerFactory.getLogger(DbWriterRunnable.class);
        private final int numberOfEntries;

        public DbWriterRunnable(int numberOfEntries) {
            this.numberOfEntries = numberOfEntries;
        }

        @Override
        public void run() {
            logger.debug("Starting thread: " + Thread.currentThread().getName());

            Session session = new ConnectionManager().connect(node, username, password, cluster);
            new DatabaseWriter().populateWithData(session, keyspace, table, columns, numberOfEntries);

            session.close();
        }
    }
}

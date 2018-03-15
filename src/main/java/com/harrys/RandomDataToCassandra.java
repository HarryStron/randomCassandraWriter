package com.harrys;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.log4j.BasicConfigurator;

public class RandomDataToCassandra {
    private final static String NODE_NAME = "node";
    private final static String CASS_USERNAME = "user";
    private final static String CASS_PASSWORD = "pass";
    private final static String CLUSTER_NAME = "cluster";
    private final static String KEYSPACE_NAME = "keyspace";
    private final static String TABLE_NAME = "table";
    private final static int ROWS_PER_RUNNABLE = 1000; //ROWS_PER_RUNNABLE * NUMBER_OF_RUNNABLES = total entries to be written
    private final static int NUMBER_OF_RUNNABLES = 100;
    private final static int THREAD_POOL_LIMIT = 100;
    private final static List<Column> COLUMNS = Arrays.asList(
                                                        new Column("example1", "'[0-9]{10}'"),
                                                        new Column("example2", "'20[0-9]{2}-0[1-9]-1[0-9] (0|1)[0-9]:[0-5][0-9]:11\\.000'")
                                                );

    public static void main(String[] args) throws IOException, InterruptedException {
        BasicConfigurator.configure();

        DatabaseWriterManager dbManager = new DatabaseWriterManager(NODE_NAME,CASS_USERNAME, CASS_PASSWORD,
                                                                    CLUSTER_NAME,KEYSPACE_NAME, TABLE_NAME, COLUMNS);

        dbManager.start(ROWS_PER_RUNNABLE, NUMBER_OF_RUNNABLES, THREAD_POOL_LIMIT);
    }
}
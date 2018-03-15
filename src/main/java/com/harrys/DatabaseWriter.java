package com.harrys;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import java.util.List;
import org.slf4j.LoggerFactory;

class DatabaseWriter {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(DatabaseWriter.class);

    void populateWithData(Session session, String keyspace, String table, List<Column> columns, int numberOfRowsToPopulate) {
        PreparedStatement insert;
        int rowCount = 0;

        String baseStatement = buildPreparedStatementStringWithoutValues(keyspace, table, columns);

        while (rowCount < numberOfRowsToPopulate) {
            insert = buildPopulatedWithRandomValuesStatement(baseStatement, session, columns);
            logger.debug("Running query: " + insert.getQueryString());

            if (session.execute(insert.bind()).wasApplied()) {
                rowCount++;
                logger.debug("Number of processed entries: " + rowCount);
            } else {
                logger.warn("Entry already in DB: " + insert.getQueryString());
            }
        }
        logger.info("All " + rowCount + " rows have been written");
    }

    private PreparedStatement buildPopulatedWithRandomValuesStatement(String baseStatement, Session session, List<Column> columns) {
        StringBuilder sbValues = new StringBuilder();
        sbValues.append("(");
        String prefix = "";
        for (Column column : columns) {
            sbValues.append(prefix);
            prefix = ",";
            sbValues.append(column.generateRandomValue());
        }
        sbValues.append(")");

        return session.prepare(baseStatement + sbValues.toString());
    }

    private String buildPreparedStatementStringWithoutValues(String keyspace, String table, List<Column> columns) {
        StringBuilder sbColumns = new StringBuilder();
        sbColumns.append("(");
        String prefix = "";
        for (Column column : columns) {
            sbColumns.append(prefix);
            prefix = ",";
            sbColumns.append(column.getName());
        }
        sbColumns.append(")");

        return "INSERT INTO " + keyspace + '.' + table + " " + sbColumns.toString() + " VALUES ";
    }
}
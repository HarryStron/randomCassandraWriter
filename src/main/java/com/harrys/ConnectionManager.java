package com.harrys;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.LoggerFactory;

class ConnectionManager {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

    Session connect(final String node, String username, String password, String clusterName) {
        Cluster cluster = Cluster.builder()
                .addContactPoint(node)
                .withCredentials(username, password)
                .withClusterName(clusterName)
                .build();

        final Metadata metadata = cluster.getMetadata();
        logger.info("Connected to cluster: " + metadata.getClusterName());

        for (final Host host : metadata.getAllHosts()) {
            logger.info("Datacenter: " + host.getDatacenter() + "; Host: " + host.getAddress() + "; Rack: " + host.getRack());
        }

        return cluster.connect();
    }
}

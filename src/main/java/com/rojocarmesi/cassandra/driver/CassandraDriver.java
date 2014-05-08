package com.rojocarmesi.cassandra.driver;

import com.datastax.driver.core.*;
import com.rojocarmesi.cassandra.exceptions.CassandraException;

import java.util.Collection;
import java.util.List;

public class CassandraDriver {

    private Cluster cluster;
    private Session session;

    /**
     * Connects to the cluster through a node
     * @param node IP address
     */
    public void connect(String node) {
        cluster = Cluster.builder()
                .addContactPoint(node)
                .build();
        Metadata metadata = cluster.getMetadata();

        System.out.println("Connected to cluster: "+metadata.getClusterName());
        for (Host host: metadata.getAllHosts()) {
            System.out.println("Datacenter: "+host.getDatacenter()+"; Host: "+host.getAddress()+"; Rack: "+host.getRack());
        }

        session = cluster.connect();
    }

    public TableMetadata getTableMetadata(String keyspace, String tablename){
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
        if(keyspaceMetadata == null){
            throw new CassandraException("Keyspace '"+keyspace+"' doesn't exist.");
        }
        TableMetadata tableMetadata = keyspaceMetadata.getTable(tablename);
        if(tableMetadata == null){
            throw new CassandraException("Table '"+tablename+"' doesn't exist.");
        }
        return tableMetadata;
    }

    public ResultSet execute(String query){
        return session.execute(query);
    }

    public List<KeyspaceMetadata> getKeyspaces(){
        return cluster.getMetadata().getKeyspaces();
    }

    public Collection<TableMetadata> getTablenames(String keyspace) {
        return cluster.getMetadata().getKeyspace(keyspace).getTables();
    }

    public void close() {
        cluster.close();
    }

}

package com.rojocarmesi.cassandra.data.filler;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;
import com.rojocarmesi.cassandra.driver.CassandraDriver;
import org.apache.commons.lang.RandomStringUtils;

import java.io.Console;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class main {

    private static final int RANDOM_LENGHT = 10;

    public static void main(String[] args) {

        //TODO: boolean lowercase
        //TODO: string length

        Scanner scan= new Scanner(System.in);

        System.out.print("Keyspace: ");
        String keyspace = scan.nextLine();

        System.out.print("Tablename: ");
        String tablename = scan.nextLine();

        int desiredRows = 0;
        System.out.print("Number of desired rows: ");
        try {
            desiredRows = Integer.parseInt(scan.nextLine());
        } catch (NumberFormatException nfe){
            System.err.println("3rd argument must be a positive number");
            System.exit(-1);
        }

        CassandraDriver cDriver = new CassandraDriver();
        cDriver.connect("127.0.0.1");

        TableMetadata tableMetadata = cDriver.getTableMetadata(keyspace, tablename);

        StringBuilder sb = null;
        for(int i=0; i<desiredRows; i++){
            sb = new StringBuilder("INSERT INTO "+keyspace+"."+tablename);
            sb.append(" (");
            for(ColumnMetadata columnMetadata: tableMetadata.getColumns()){
                sb.append(columnMetadata.getName());
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length()-1);
            sb.deleteCharAt(sb.length()-1);
            sb.append(") VALUES (");
            int randomInt = (int) (Math.random()*RANDOM_LENGHT);
            randomInt++;
            List<DataType> stringDataTypes = new ArrayList<>();
            stringDataTypes.add(DataType.ascii());
            stringDataTypes.add(DataType.text());
            stringDataTypes.add(DataType.varchar());
            stringDataTypes.add(DataType.blob());
            stringDataTypes.add(DataType.inet());
            List<DataType> uuidDataTypes = new ArrayList<>();
            uuidDataTypes.add(DataType.timeuuid());
            uuidDataTypes.add(DataType.uuid());
            for(ColumnMetadata columnMetadata: tableMetadata.getColumns()){
                if(stringDataTypes.contains(columnMetadata.getType())) {
                    sb.append("'").append(RandomStringUtils.randomAlphabetic(randomInt)).append("'");
                } else if(columnMetadata.getType() == DataType.cboolean()) {
                    if (randomInt % 2 == 0) {
                        sb.append("True");
                    } else {
                        sb.append("False");
                    }
                } else if(uuidDataTypes.contains(columnMetadata.getType())) {
                    sb.append("'").append(UUID.randomUUID().toString()).append("'");
                } else {
                    sb.append(randomInt);
                }
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length()-1);
            sb.deleteCharAt(sb.length()-1);
            sb.append(")");
            System.out.println("Query: "+sb.toString());
            ResultSet result = cDriver.execute(sb.toString());
            System.out.println("Result "+i+": "+result.toString());
        }

        ResultSet result = cDriver.execute("SELECT COUNT(*) FROM "+keyspace+"."+tablename);
        System.out.println("Final result: COUNT=" + result.one().toString());

        cDriver.close();
    }

}
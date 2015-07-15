package com.rojocarmesi.cassandra.data.filler;

import com.datastax.driver.core.*;
import com.rojocarmesi.cassandra.driver.CassandraDriver;
import org.apache.commons.lang.RandomStringUtils;

import java.util.*;

public class main {

    public static void main(String[] args) {

        //DONE: boolean lowercase
        //DONE: loop
        //DONE: sizes
        //DONE: allow patterns
        //DONE: number ranges
        //DONE: allow sets
        //TODO: bug
        //TODO: requiring data according to type
        //TODO: random with jfairy
        //TODO: secuencial type when numeric
        //TODO: refactor
        //TODO: tests
        //DONE: show keyspaces and tablenames

        Scanner scan = new Scanner(System.in);

        CassandraDriver cDriver = new CassandraDriver();
        cDriver.connect("127.0.0.1");

        boolean repeat;

        do {

            // SHOW KEYSPACES
            List<KeyspaceMetadata> keyspaces = cDriver.getKeyspaces();
            System.out.println("KEYSPACES: [");
            for(KeyspaceMetadata keyspace: keyspaces){
                if(!keyspace.getName().startsWith("system")) {
                    System.out.print(keyspace.getName() + " ");
                }
            }
            System.out.println("]");

            //GET KEYSPACE
            System.out.print("Keyspace: ");
            String keyspace = scan.nextLine();

            // SHOW TABLENAMES
            Collection<TableMetadata> tablenames = cDriver.getTablenames(keyspace);
            System.out.println("TABLENAMES: [");
            for(TableMetadata tablename: tablenames){
                System.out.print(tablename.getName()+" ");
            }
            System.out.println("]");

            // GET TABLENAME
            System.out.print("Tablename: ");
            String tablename = scan.nextLine();

            int desiredRows = 0;
            System.out.print("Number of desired rows: ");
            try {
                desiredRows = Integer.parseInt(scan.nextLine());
            } catch (NumberFormatException nfe) {
                System.err.println("3rd argument must be a positive number");
                System.exit(-1);
            }

            TableMetadata tableMetadata = cDriver.getTableMetadata(keyspace, tablename);

            Map<String, Integer[]> sizes = new HashMap<>();
            Map<String, Boolean> lowercase = new HashMap<>();
            Map<String, String[]> strings = new HashMap<>();
            Map<String, String> patterns = new HashMap<>();

            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                // METADATA
                String colName = columnMetadata.getName();
                String colType = columnMetadata.getType().getName().name();

                // SIZE
                System.out.print("Size for " + colName + "(" + colType + ") [min - max]: ");
                String[] rangeArray = scan.nextLine().trim().split("-");
                sizes.put(colName, new Integer[]{Integer.parseInt(rangeArray[0].trim()), Integer.parseInt(rangeArray[1].trim())});

                // GET SET OF STRINGS
                System.out.print("Define set of words [str1, str2, ... strN]: ");
                String strs = scan.nextLine();
                if(strs.contains(",")) {
                    strings.put(colName, strs.trim().split(","));
                } else {
                    strings.put(colName, new String[]{strs.trim()});
                }

                // GET PATTERN
                System.out.print("Define pattern [Example: *@*.com]: ");
                String pattern = scan.nextLine();
                patterns.put(colName, pattern);

                // LOWERCASE
                System.out.print("lowercase? [Y/n]: ");
                String toLowercase = scan.nextLine();
                if(toLowercase.trim().toLowerCase().startsWith("y")){
                    lowercase.put(colName, Boolean.TRUE);
                } else {
                    lowercase.put(colName, Boolean.FALSE);
                }
            }

            StringBuilder sb;
            for (int i = 0; i < desiredRows; i++) {
                sb = new StringBuilder("INSERT INTO " + keyspace + "." + tablename);
                sb.append(" (");
                for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                    sb.append(columnMetadata.getName());
                    sb.append(", ");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(sb.length() - 1);
                sb.append(") VALUES (");
                List<DataType> stringDataTypes = new ArrayList<>();
                stringDataTypes.add(DataType.ascii());
                stringDataTypes.add(DataType.text());
                stringDataTypes.add(DataType.varchar());
                stringDataTypes.add(DataType.blob());
                stringDataTypes.add(DataType.inet());
                List<DataType> uuidDataTypes = new ArrayList<>();
                uuidDataTypes.add(DataType.timeuuid());
                uuidDataTypes.add(DataType.uuid());
                for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                    Integer min_size = sizes.get(columnMetadata.getName())[0];
                    Integer max_size = sizes.get(columnMetadata.getName())[1];
                    int range = max_size - min_size;
                    int randomInt = (int) (Math.random() * range)+1+min_size;
                    randomInt++;
                    if (stringDataTypes.contains(columnMetadata.getType())) {
                        String newString = RandomStringUtils.randomAlphabetic(randomInt);
                        if(lowercase.get(columnMetadata.getName())){
                            newString = newString.toLowerCase();
                        }
                        String[] strsSet = strings.get(columnMetadata.getName());
                        if(strsSet[0].length()>0){
                            randomInt = (int) (Math.random() * strsSet.length);
                            newString = strsSet[randomInt];
                        }
                        if(patterns.get(columnMetadata.getName()).length() > 0){
                            newString = "";
                            String pattern = patterns.get(columnMetadata.getName());
                            /*int numberOfAsteriks = 0;
                            for(int j=0; j<pattern.length(); j++){
                                if(pattern.charAt(j) == '*'){
                                    numberOfAsteriks++;
                                }
                            }*/
                            if(pattern.startsWith("*")){
                                newString+=RandomStringUtils.randomAlphabetic(randomInt);
                            }
                            String[] fixedParts = pattern.split("\\*");
                            for(int j=0; j<fixedParts.length; j++){
                                newString+=fixedParts[j];
                                if(j < fixedParts.length-1){
                                    newString+=RandomStringUtils.randomAlphabetic(randomInt);
                                }
                            }
                            if(pattern.startsWith("*")){
                                newString+=RandomStringUtils.randomAlphabetic(randomInt);
                            }
                        }
                        sb.append("'").append(newString).append("'");
                    } else if (columnMetadata.getType() == DataType.cboolean()) {
                        if (randomInt % 2 == 0) {
                            sb.append("True");
                        } else {
                            sb.append("False");
                        }
                    } else if (uuidDataTypes.contains(columnMetadata.getType())) {
                        sb.append("'").append(UUID.randomUUID().toString()).append("'");
                    } else {
                        sb.append(randomInt);
                    }
                    sb.append(", ");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(sb.length() - 1);
                sb.append(")");
                System.out.println("Query: " + sb.toString());
                ResultSet result = cDriver.execute(sb.toString());
                System.out.println("Result " + i + ": " + result.toString());
            }

            ResultSet result = cDriver.execute("SELECT COUNT(*) FROM " + keyspace + "." + tablename);
            System.out.println("Final result: COUNT=" + result.one().getLong(0));

            System.out.print("Repeat process? [Y/n]: ");

            String answer = scan.nextLine();

            if(answer.trim().toLowerCase().startsWith("y")){
                repeat = true;
            } else {
                repeat = false;
            }

        } while(repeat);

        cDriver.close();
    }

}
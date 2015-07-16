package io.miguel0afd.cassandra.data.filler.data.filler;

import com.datastax.driver.core.*;
import io.miguel0afd.cassandra.data.filler.driver.CassandraDriver;
import org.apache.commons.lang.RandomStringUtils;

import java.util.*;

import io.codearte.jfairy.Fairy;

public class main {

    public static void main(String[] args) {

        //DONE: show keyspaces and tablenames
        //DONE: loop
        //DONE: sizes
        //DONE: number ranges
        //DONE: allow sets
        //DONE: allow patterns
        //DONE: boolean lowercase
        //DONE: requiring data according to type
        //DONE: sequential type when numeric
        //DONE: basic random data with jfairy
        //TODO: advanced random data with jfairy
        //TODO: refactor
        //TODO: tests

        List<DataType> stringDataTypes = new ArrayList<>();
        stringDataTypes.add(DataType.ascii());
        stringDataTypes.add(DataType.text());
        stringDataTypes.add(DataType.varchar());
        stringDataTypes.add(DataType.blob());
        stringDataTypes.add(DataType.inet());

        List<DataType> uuidDataTypes = new ArrayList<>();
        uuidDataTypes.add(DataType.timeuuid());
        uuidDataTypes.add(DataType.uuid());

        List<DataType> numericTypes = new ArrayList<>();
        numericTypes.add(DataType.bigint());
        numericTypes.add(DataType.cdouble());
        numericTypes.add(DataType.cfloat());
        numericTypes.add(DataType.cint());
        numericTypes.add(DataType.counter());
        numericTypes.add(DataType.decimal());
        numericTypes.add(DataType.timestamp());
        numericTypes.add(DataType.varint());

        Scanner scan = new Scanner(System.in);

        Fairy fairy = Fairy.create();

        System.out.println("=== CASSANDRA DATA FILLER ===");
        System.out.print("Host: ");
        String host = scan.nextLine();

        CassandraDriver cDriver = new CassandraDriver();
        cDriver.connect(host);

        boolean repeat;

        do {

            // SHOW KEYSPACES
            List<KeyspaceMetadata> keyspaces = cDriver.getKeyspaces();
            System.out.print("KEYSPACES: [ ");
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
            System.out.print("TABLENAMES: [ ");
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
                System.err.println("Number must be positive");
                System.exit(-1);
            }

            TableMetadata tableMetadata = cDriver.getTableMetadata(keyspace, tablename);

            Map<String, Integer[]> ranges = new HashMap<>();
            Map<String, Boolean> sequential = new HashMap<>();
            Map<String, String[]> strings = new HashMap<>();
            Map<String, String> patterns = new HashMap<>();
            Map<String, Boolean> lowercase = new HashMap<>();

            for (ColumnMetadata columnMetadata: tableMetadata.getColumns()) {
                // METADATA
                String colName = columnMetadata.getName();
                String colType = columnMetadata.getType().getName().name();

                // SIZE
                System.out.println(" - Data for " + colName + " (Type: " + colType + "): ");
                if(numericTypes.contains(columnMetadata.getType())){
                    System.out.print("Range [min - max] (Default: 0-" + (desiredRows-1) + "): ");
                    String sizeInString = scan.nextLine().trim();
                    if(sizeInString.isEmpty()){
                        ranges.put(colName, new Integer[] {0, desiredRows-1});
                    } else {
                        String[] rangeArray = sizeInString.split("-");
                        ranges.put(colName, new Integer[] {Integer.parseInt(rangeArray[0].trim()),
                                Integer.parseInt(rangeArray[1].trim())});
                    }
                    System.out.print("Sequential? [Y/n] (Default: No): ");
                    String isSequential = scan.nextLine();
                    if(isSequential.trim().toLowerCase().startsWith("y")){
                        sequential.put(colName, Boolean.TRUE);
                    } else {
                        sequential.put(colName, Boolean.FALSE);
                    }
                } else {
                    ranges.put(colName, new Integer[] {0, desiredRows-1});
                    sequential.put(colName, Boolean.FALSE);
                }

                if (stringDataTypes.contains(columnMetadata.getType())){
                    // GET SET OF STRINGS
                    String[] randomSet = new String[2];
                    String city1 = fairy.person().getAddress().getCity();
                    String city2 = fairy.person().getAddress().getCity();
                    while(city2.equalsIgnoreCase(city1)){
                        city2 = fairy.person().getAddress().getCity();
                    }
                    randomSet[0] = city1;
                    randomSet[1] = city2;

                    System.out.print("Define set of words [str1, str2, ... strN] (Default: "
                            + Arrays.toString(randomSet) + "): ");
                    String strs = scan.nextLine();
                    if(strs.isEmpty()){
                        strings.put(colName, randomSet);
                    } else if(strs.contains(",")) {
                        strings.put(colName, strs.trim().split(","));
                    } else {
                        strings.put(colName, new String[]{strs.trim()});
                    }

                    // GET PATTERN
                    System.out.print("Define pattern [Example: *@*.com] (Default: *): ");
                    String pattern = scan.nextLine();
                    patterns.put(colName, pattern);

                    // LOWERCASE
                    System.out.print("Lowercase? [Y/n] (Default: No): ");
                    String toLowercase = scan.nextLine();
                    if(toLowercase.trim().toLowerCase().startsWith("y")){
                        lowercase.put(colName, Boolean.TRUE);
                    } else {
                        lowercase.put(colName, Boolean.FALSE);
                    }
                } else {
                    strings.put(colName, new String[]{""});
                    patterns.put(colName, "");
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
                for (ColumnMetadata columnMetadata: tableMetadata.getColumns()) {
                    Integer min_size = ranges.get(columnMetadata.getName())[0];
                    Integer max_size = ranges.get(columnMetadata.getName())[1];
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
                    } else if (numericTypes.contains(columnMetadata.getType())){
                        if(sequential.get(columnMetadata.getName())){
                            sb.append(i+min_size);
                        } else {
                            sb.append(randomInt);
                        }
                    } else {
                        sb.append("''");
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
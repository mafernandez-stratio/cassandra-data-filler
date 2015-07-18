package io.miguel0afd.cassandra.data.filler.data.filler;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import io.codearte.jfairy.producer.company.Company;
import io.miguel0afd.cassandra.data.filler.driver.CassandraDriver;
import org.apache.commons.lang.RandomStringUtils;

import java.util.*;

import io.codearte.jfairy.Fairy;

public class App {

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
        //DONE: advanced random data with jfairy
        //DONE: improve decimal number production
        //TODO: decimal precision
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

        List<DataType> decimalTypes = new ArrayList<>();
        decimalTypes.add(DataType.cfloat());
        decimalTypes.add(DataType.cdouble());

        List<DataType> numericTypes = new ArrayList<>(decimalTypes);
        numericTypes.add(DataType.bigint());
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

            boolean advancedMode = false;
            System.out.print("Advanced mode? [Y/n] (Default: No): ");
            String useAdvancedMode = scan.nextLine();
            if(useAdvancedMode.trim().toLowerCase().startsWith("y")){
                advancedMode = true;
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

                    String defaultSet = Arrays.toString(randomSet);
                    if(advancedMode){
                        defaultSet = "ENTER advance mode";
                    }
                    System.out.print("Define set of words [str1, str2, ... strN] (Default: "
                            + defaultSet + "): ");
                    String strs = scan.nextLine();
                    if(strs.isEmpty()){
                        strings.put(colName, randomSet);
                        if(advancedMode){
                            randomSet = advanceGeneration(scan, fairy);
                        }
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

            System.out.println("--------------------------------");
            System.out.print("Show rows? [Y/n] (Default: No): ");
            boolean showRows = false;
            String shouldShowRows = scan.nextLine();
            if(shouldShowRows.trim().toLowerCase().startsWith("y")){
                showRows = true;
            }

            StringBuilder sb;
            sb = new StringBuilder("INSERT INTO " + keyspace + "." + tablename);
            sb.append(" (");
            Iterator<ColumnMetadata> iter = tableMetadata.getColumns().iterator();
            while(iter.hasNext()){
                sb.append(iter.next().getName());
                if(iter.hasNext()){
                    sb.append(", ");
                }
            }
            sb.append(") VALUES (");
            String initialChars = sb.toString();

            System.out.println("Inserting data");

            for (int i = 0; i < desiredRows; i++) {
                sb = new StringBuilder(initialChars);

                iter = tableMetadata.getColumns().iterator();
                while(iter.hasNext()){
                    ColumnMetadata columnMetadata = iter.next();

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
                        if(sequential.get(columnMetadata.getName())) {
                            sb.append(i + min_size);
                        } else if(decimalTypes.contains(columnMetadata.getType())){
                            float randomDecimal = (float) ((Math.random() * range)+1+min_size);
                            sb.append(randomDecimal);
                        } else {
                            sb.append(randomInt);
                        }
                    } else {
                        sb.append("''");
                    }
                    if(iter.hasNext()){
                        sb.append(", ");
                    }
                }
                sb.append(")");

                try {
                    ResultSet result = cDriver.execute(sb.toString());
                    if(showRows){
                        System.out.println("Query: " + sb.toString());
                        System.out.println("Result " + i + ": " + result.toString());
                    }
                } catch (DriverException de){
                    System.out.println(" === Driver Exception === ");
                    System.out.print("Try again? [Y/n] (Default: No): ");

                    String answer = scan.nextLine();
                    boolean tryAgain = false;
                    if(answer.trim().toLowerCase().startsWith("y")){
                        tryAgain = true;
                    }
                    assert tryAgain;
                }
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

    private static String[] advanceGeneration(Scanner scan, Fairy fairy) {
        System.out.println(" === Advanced Node === ");
        System.out.print("Size?: ");
        Integer advancedSize = Integer.parseInt(scan.nextLine());
        String[] randomSet = new String[advancedSize];
        System.out.println("1.- Company");
        System.out.println("2.- Credit Card");
        System.out.println("3.- Network");
        System.out.println("4.- Person");
        System.out.println("5.- Address");
        System.out.println("6.- Text");
        System.out.print("Choose: ");
        int firstOption = Integer.parseInt(scan.nextLine());
        int secondOption;
        switch (firstOption){
        case 1:
            System.out.println("1.- Domain");
            System.out.println("2.- Email");
            System.out.println("3.- Name");
            System.out.println("4.- URL");
            System.out.println("5.- VAT");
            System.out.print("Choose: ");
            secondOption = Integer.parseInt(scan.nextLine());
            switch (secondOption){
            case 1:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.company().domain();
                }
                break;
            case 2:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.company().email();
                }
                break;
            case 3:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.company().name();
                }
                break;
            case 4:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.company().url();
                }
                break;
            case 5:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.company().vatIdentificationNumber();
                }
                break;
            }
            break;
        case 2:
            System.out.println("1.- Expiry Date");
            System.out.println("2.- Vendor");
            System.out.print("Choose: ");
            secondOption = Integer.parseInt(scan.nextLine());
            switch (secondOption){
            case 1:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.creditCard().expiryDateAsString();
                }
                break;
            case 2:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.creditCard().vendor();
                }
                break;
            }
            break;
        case 3:
            fairy.networkProducer();
            System.out.println("1.- IP Address");
            System.out.print("Choose: ");
            secondOption = Integer.parseInt(scan.nextLine());
            switch (secondOption){
            case 1:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.networkProducer().ipAddress();
                }
                break;
            }
            break;
        case 4:
            fairy.person();
            System.out.println("1.- Company Email");
            System.out.println("2.- Email");
            System.out.println("3.- First Name");
            System.out.println("4.- Full Name");
            System.out.println("5.- Last name");
            System.out.println("6.- National Identification Number");
            System.out.println("7.- Passport Number");
            System.out.println("8.- Password");
            System.out.println("9.- Telephone");
            System.out.println("10.- Username");
            System.out.print("Choose: ");
            secondOption = Integer.parseInt(scan.nextLine());
            switch (secondOption){
            case 1:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().companyEmail();
                }
                break;
            case 2:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().email();
                }
                break;
            case 3:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().firstName();
                }
                break;
            case 4:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().fullName();
                }
                break;
            case 5:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().lastName();
                }
                break;
            case 6:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().nationalIdentificationNumber();
                }
                break;
            case 7:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().passportNumber();
                }
                break;
            case 8:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().password();
                }
                break;
            case 9:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().telephoneNumber();
                }
                break;
            case 10:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().username();
                }
                break;
            }
            break;
        case 5:
            fairy.person().getAddress();
            System.out.println("1.- Apartment Number");
            System.out.println("2.- City");
            System.out.println("3.- Postal Code");
            System.out.println("4.- Street");
            System.out.println("5.- Street Number");
            System.out.print("Choose: ");
            secondOption = Integer.parseInt(scan.nextLine());
            switch (secondOption){
            case 1:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().getAddress().apartmentNumber();
                }
                break;
            case 2:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().getAddress().getCity();
                }
                break;
            case 3:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().getAddress().getPostalCode();
                }
                break;
            case 4:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().getAddress().street();
                }
                break;
            case 5:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.person().getAddress().streetNumber();
                }
                break;
            }
            break;
        case 6:
            System.out.println("1.- Random");
            System.out.println("2.- Sentence");
            System.out.println("3.- Word");
            System.out.print("Choose: ");
            secondOption = Integer.parseInt(scan.nextLine());
            switch (secondOption){
            case 1:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.textProducer().randomString(advancedSize);
                }
                break;
            case 2:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.textProducer().sentence();
                }
                break;
            case 3:
                for(int i = 0; i < advancedSize; i++){
                    randomSet[i] = fairy.textProducer().word();
                }
                break;
            }
            break;
        }
        return randomSet;
    }

}
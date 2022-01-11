import java.sql.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.sql.Date;
import java.util.logging.Logger;
import java.io.FileReader;
import org.json.*;

class MainCategory {
    String business_id, category;
    MainCategory(String business_id, String category) {
        this.business_id = business_id;
        this.category = category;
    }
}

class SubCategory {
    String business_id, sub_category;
    SubCategory(String business_id, String sub_category) {
        this.business_id = business_id;
        this.sub_category = sub_category;
    }
}

class Attribute {
    String business_id, attribute;
    Attribute(String business_id, String attribute) {
        this.business_id = business_id;
        this.attribute = attribute;
    }
}


public class populate {
    private static Connection con = null;
    private static Set<String> mainCategories_set = new HashSet<>(Arrays.asList("Active Life","Arts & Entertainment","Automotive","Car Rental","Cafes",
            "Beauty & Spas","Convenience Stores", "Dentists","Doctors","Drugstores","Department Stores","Education","Event Planning & Services",
            "Flowers & Gifts","Food", "Health & Medical","Home Services","Home & Garden","Hospitals","Hotels & Travel","Hardware Stores","Grocery",
            "Medical Centers", "Nurseries & Gardening","Nightlife","Restaurants","Shopping","Transportation"));


    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, ParseException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","system", "oracle");
        Delete_tables();
        populate_Business(args[0]);//args[0]
        populate_Users(args[3]);         //args[3]
        populate_Review(args[1]);      //args[1]
//        populate_CheckIn("src/HW/yelp_checkin.json");    //args[2]

        System.out.println("Creating Indexes");
        Statement stmt = con.createStatement();
        stmt.executeUpdate("CREATE INDEX MAINCATEINDEX ON CATEGORIES(CATEGORY)");
        stmt.executeUpdate("CREATE INDEX MAINBIDINDEX ON CATEGORIES(BUSINESS_ID)");
        stmt.executeUpdate("CREATE INDEX SUBCATEINDEX ON SUB_CATEGORIES(SUB_CATEGORY)");
        stmt.executeUpdate("CREATE INDEX SUBCATEBIDINDEX ON SUB_CATEGORIES(BUSINESS_ID)");
        stmt.executeUpdate("CREATE INDEX ATTRINDEX ON ATTRIBUTES(ATTRIBUTE)");
        stmt.executeUpdate("CREATE INDEX ATTRBIDINDEX ON ATTRIBUTES(BUSINESS_ID)");
        stmt.executeUpdate("CREATE INDEX REVIEWBIDINDEX ON REVIEW(BUSINESS_ID)");
        stmt.executeUpdate("CREATE INDEX REVIEWUIDINDEX ON REVIEW(USER_ID)");
        con.close();
    }

    public static Connection getConnect() throws SQLException, ClassNotFoundException{
        System.out.println("Checking JDBC...");
        Class.forName("oracle.jdbc.driver.OracleDriver");
        System.out.println("Connecting to database...");
        return DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","system", "oracle");
    }

    private static void populate_Business(String file) {
        System.out.println("Process Business");
        file = "src/" + file;
        try{
            int c = 0;
            ArrayList<MainCategory> mainCategory_list = new ArrayList<>();
            ArrayList<SubCategory> subCategory_list = new ArrayList<>();
            ArrayList<Attribute> attribute_list = new ArrayList<>();
//            ArrayList<BusinessHour> businessHour_list = new ArrayList<>();
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line = null;
            PreparedStatement stmt = con.prepareStatement("INSERT INTO BUSINESS VALUES(?,?,?,?,?,?,?)");

            while((line = reader.readLine()) != null){
                JSONObject j = new JSONObject(line);

                String business_id = j.getString("business_id");
                JSONArray c_array = j.getJSONArray("categories");
                for(int i = 0; i < c_array.length(); i++){
                    String cate = c_array.getString(i);
                    if(mainCategories_set.contains(cate)){
                        mainCategory_list.add(new MainCategory(business_id, cate));
                    }
                    else{
                        subCategory_list.add(new SubCategory(business_id, cate));
                    }
                }

                JSONObject attr = j.getJSONObject("attributes");
                Iterator<String> keys = attr.keys();

                while(keys.hasNext()){
                    String key = keys.next();
                    if(attr.get(key) instanceof JSONObject){
                        JSONObject attr1 = attr.getJSONObject(key);
                        Iterator<String> keys1 = attr1.keys();
                        while(keys1.hasNext()){
                            String key1 = keys1.next();
                            attribute_list.add(new Attribute(business_id, key + '_' + key1 + '_' + attr1.get(key1)));
                        }
                    }
                    else{
                        attribute_list.add(new Attribute(business_id, key + "_" + attr.get(key)));
                    }
                }

                stmt.setString(1, business_id);
                stmt.setString(2, j.getString("full_address"));
                stmt.setString(3, j.getString("city"));
                stmt.setString(4, j.getString("state"));
                stmt.setInt(5, j.getInt("review_count"));
                stmt.setString(6, j.getString("name"));
                stmt.setDouble(7, j.getDouble("stars"));
                stmt.addBatch();
                c++;
                if(c > 1000) {
                    stmt.executeBatch();
                    c = 0;
                }
            }
            if(c > 0){
                stmt.executeBatch();
            }
            stmt.close();
            reader.close();
            populate_MainCategory(mainCategory_list);
            populate_SubCategory(subCategory_list);
            populate_Attribute(attribute_list);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void populate_MainCategory(ArrayList<MainCategory> mainCategory_list) {
        System.out.println("Process MainCategory");
        int c = 0;
        try {
            PreparedStatement stmt = con.prepareStatement("INSERT INTO CATEGORIES VALUES(?,?)");
            for(MainCategory i : mainCategory_list){
                stmt.setString(1, i.business_id);
                stmt.setString(2, i.category);
                stmt.addBatch();
                c++;
                if(c > 1000){
                    stmt.executeBatch();
                    c = 0;
                }
            }
            if(c > 0){
                stmt.executeBatch();
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void populate_SubCategory(ArrayList<SubCategory> subCategory_list) {
        System.out.println("Process SubCategory");
        int c = 0;
        try {
            PreparedStatement stmt = con.prepareStatement("INSERT INTO SUB_CATEGORIES VALUES(?,?)");
            for(SubCategory i : subCategory_list){
                stmt.setString(1, i.business_id);
                stmt.setString(2, i.sub_category);
                stmt.addBatch();
                c++;
                if(c > 1000){
                    stmt.executeBatch();
                    c = 0;
                }
            }
            if(c > 0){
                stmt.executeBatch();
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void populate_Attribute(ArrayList<Attribute> attribute_list) {
        System.out.println("Process Attribute");
        int c = 0;
        try {
            PreparedStatement stmt = con.prepareStatement("INSERT INTO ATTRIBUTES VALUES(?,?)");
            for(Attribute i : attribute_list){
                stmt.setString(1, i.business_id);
                stmt.setString(2, i.attribute);
                stmt.addBatch();
                c++;
                if(c > 1000){
                    stmt.executeBatch();
                    c = 0;
                }
            }
            if(c > 0){
                stmt.executeBatch();
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void populate_Users(String file) throws IOException, SQLException, ParseException {
        System.out.println("Process User");
        file = "src/" + file;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        PreparedStatement stmt = con.prepareStatement("INSERT INTO USERS VALUES(?,?,?,?,?,?,?)");
        int c = 0;
        SimpleDateFormat dateformate = new SimpleDateFormat("yyyy-MM");
        while((line = reader.readLine()) != null) {
            JSONObject j = new JSONObject(line);
            stmt.setString(1, j.getString("user_id"));
            java.util.Date date = dateformate.parse(j.getString("yelping_since"));
            stmt.setDate(2, new Date(date.getTime()));
            stmt.setInt(3, j.getInt("review_count"));
            stmt.setString(4, j.getString("name"));
            stmt.setDouble(5, j.getDouble("average_stars"));

            JSONObject vote = j.getJSONObject("votes");
            int vote_count = vote.getInt("funny") + vote.getInt("useful") + vote.getInt("cool");
            stmt.setInt(6, vote_count);
            int friends = j.getJSONArray("friends").length();
            stmt.setDouble(7, friends);
            stmt.addBatch();
            c++;
            if(c > 1000) {
                stmt.executeBatch();
                c = 0;
            }
        }
        if(c > 0){
            stmt.executeBatch();
        }
        stmt.close();
        reader.close();
    }

    private static void populate_Review(String file) throws SQLException, IOException, ParseException {
        System.out.println("Process Review");
        file = "src/" + file;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        PreparedStatement stmt = con.prepareStatement("INSERT INTO REVIEW VALUES(?,?,?,?,?,?,?)");
        int c = 0;
        SimpleDateFormat dateformate = new SimpleDateFormat("yyyy-MM-dd");
        while((line = reader.readLine()) != null) {
            JSONObject j = new JSONObject(line);
            stmt.setString(1, j.getString("review_id"));
            stmt.setString(2, j.getString("user_id"));
            stmt.setString(3, j.getString("business_id"));
            stmt.setInt(4, j.getInt("stars"));

            java.util.Date date = dateformate.parse(j.getString("date"));
            stmt.setDate(5, new Date(date.getTime()));
            stmt.setString(6, j.getString("text"));
            JSONObject vote = j.getJSONObject("votes");
            int vote_count = vote.getInt("useful") + vote.getInt("funny") + vote.getInt("cool");
            stmt.setInt(7, vote_count);
            stmt.addBatch();
            c++;
            if(c > 1000) {
                stmt.executeBatch();
                c = 0;
            }
        }
        if(c > 0){
            stmt.executeBatch();
        }
        stmt.close();
        reader.close();
    }

    private static void populate_CheckIn(String file) throws SQLException, IOException {
        System.out.println("Process CheckIn");
        file = "src/" + file;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        PreparedStatement stmt = con.prepareStatement("INSERT INTO CHECKIN VALUES(?,?,?)");
        int c = 0;
        while((line = reader.readLine()) != null) {
            JSONObject j = new JSONObject(line);

            JSONObject info = j.getJSONObject("checkin_info");
            Iterator<String> keys = info.keys();
            while(keys.hasNext()){
                String key = keys.next();
                stmt.setString(1, j.getString("business_id"));
                stmt.setString(2, key);
                stmt.setInt(3, info.getInt(key));
                stmt.addBatch();
            }
            c++;
            if(c > 500) {
                stmt.executeBatch();
                c = 0;
            }
        }
        if(c > 0){
            stmt.executeBatch();
        }
        stmt.close();
        reader.close();
    }

    private static void Delete_tables(){
        try {
            Statement stmt = con.createStatement();
//            System.out.println("Drop Indexes");
//            stmt.executeUpdate("DROP INDEX MAINCATEINDEX");
//            stmt.executeUpdate("DROP INDEX MAINBIDINDEX");
//            stmt.executeUpdate("DROP INDEX SUBCATEINDEX");
//            stmt.executeUpdate("DROP INDEX SUBCATEBIDINDEX");
//            stmt.executeUpdate("DROP INDEX ATTRINDEX");
//            stmt.executeUpdate("DROP INDEX ATTRBIDINDEX");
//            stmt.executeUpdate("DROP INDEX REVIEWBIDINDEX");
//            stmt.executeUpdate("DROP INDEX REVIEWUIDINDEX");

            System.out.println("Clean Tables");
//            stmt.executeUpdate("DELETE FROM CHECKIN");
//            stmt.executeUpdate("DELETE FROM CATEGORIES");
//            stmt.executeUpdate("DELETE FROM SUB_CATEGORIES");
//            stmt.executeUpdate("DELETE FROM ATTRIBUTES");
//            stmt.executeUpdate("DELETE FROM REVIEW");
//            stmt.executeUpdate("DELETE FROM Business");
//            stmt.executeUpdate("DELETE FROM USERS");

            stmt.executeUpdate("DROP TABLE CHECKIN");
            stmt.executeUpdate("DROP TABLE CATEGORIES");
            stmt.executeUpdate("DROP TABLE SUB_CATEGORIES");
            stmt.executeUpdate("DROP TABLE ATTRIBUTES");
            stmt.executeUpdate("DROP TABLE REVIEW");
            stmt.executeUpdate("DROP TABLE Business");
            stmt.executeUpdate("DROP TABLE USERS");

            stmt.executeUpdate("CREATE TABLE BUSINESS(\n" +
                    "    BUSINESS_ID VARCHAR(50) PRIMARY KEY,\n" +
                    "    FULL_ADDRESS VARCHAR(300) NOT NULL,\n" +
                    "    CITY VARCHAR(30) NOT NULL,\n" +
                    "    STATE VARCHAR(30) NOT NULL,\n" +
                    "    REVIEW_COUNT NUMBER(10) NOT NULL,\n" +
                    "    NAME VARCHAR(200) NOT NULL,\n" +
                    "    STARS NUMBER(2,1) NOT NULL\n" +
                    ")");
            stmt.executeUpdate("CREATE TABLE CATEGORIES(\n" +
                    "    BUSINESS_ID VARCHAR(50) NOT NULL,\n" +
                    "    CATEGORY VARCHAR(50) NOT NULL,\n" +
                    "    PRIMARY KEY(BUSINESS_ID, CATEGORY),\n" +
                    "    FOREIGN KEY(BUSINESS_ID) REFERENCES BUSINESS(BUSINESS_ID) ON DELETE CASCADE\n" +
                    ")");
            stmt.executeUpdate("CREATE TABLE SUB_CATEGORIES(\n" +
                    "    BUSINESS_ID VARCHAR(50) NOT NULL,\n" +
                    "    SUB_CATEGORY VARCHAR(50) NOT NULL,\n" +
                    "    PRIMARY KEY(BUSINESS_ID, SUB_CATEGORY),\n" +
                    "    FOREIGN KEY(BUSINESS_ID) REFERENCES BUSINESS(BUSINESS_ID) ON DELETE CASCADE\n" +
                    ")");
            stmt.executeUpdate("CREATE TABLE ATTRIBUTES(\n" +
                    "    BUSINESS_ID VARCHAR(50) NOT NULL,\n" +
                    "    ATTRIBUTE VARCHAR(50) NOT NULL,\n" +
                    "    PRIMARY KEY(BUSINESS_ID, ATTRIBUTE),\n" +
                    "    FOREIGN KEY(BUSINESS_ID) REFERENCES BUSINESS(BUSINESS_ID) ON DELETE CASCADE\n" +
                    ")");
            stmt.executeUpdate("CREATE TABLE USERS(\n" +
                    "    USER_ID VARCHAR(50) PRIMARY KEY,\n" +
                    "    YELPING_SINCE DATE NOT NULL,\n" +
                    "    REVIEW_COUNT NUMBER(10) NOT NULL,\n" +
                    "    NAME VARCHAR(50) NOT NULL,\n" +
                    "    AVERAGE_STARS NUMBER(20,16) NOT NULL,\n" +
                    "    VOTE_COUNT NUMBER(10) NOT NULL,\n" +
                    "    NUMBER_FRIEND NUMBER(10) NOT NULL\n" +
                    ")");
            stmt.executeUpdate("CREATE TABLE REVIEW(\n" +
                    "    REVIEW_ID VARCHAR(50) PRIMARY KEY,\n" +
                    "    USER_ID VARCHAR(50) NOT NULL,\n" +
                    "    BUSINESS_ID VARCHAR(50) NOT NULL,\n" +
                    "    STAR NUMBER(1) NOT NULL,\n" +
                    "    R_DATE DATE NOT NULL,\n" +
                    "    TEXT LONG,\n" +
                    "    VOTE_COUNT NUMBER(10) NOT NULL,\n" +
                    "    FOREIGN KEY(USER_ID) REFERENCES USERS(USER_ID) ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY(BUSINESS_ID) REFERENCES BUSINESS(BUSINESS_ID) ON DELETE CASCADE\n" +
                    ")");
            stmt.executeUpdate("CREATE TABLE CHECKIN(\n" +
                    "    BUSINESS_ID VARCHAR(50),\n" +
                    "    DAYTIME VARCHAR(4) NOT NULL,\n" +
                    "    COUNT NUMBER(10) NOT NULL,\n" +
                    "    PRIMARY KEY(BUSINESS_ID, DAYTIME),\n" +
                    "    FOREIGN KEY(BUSINESS_ID) REFERENCES BUSINESS(BUSINESS_ID) ON DELETE CASCADE\n" +
                    ")");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}


package accessDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class MySQLAccess {
	
    public static void main(String[] args) throws Exception {
    	
    	Connection connect = null;
    	Statement statement = null;
    	ResultSet resultSet = null, colResultSet = null;
    	MySQLAccess access = new MySQLAccess();
        try (NeodbManager neo = new NeodbManager("bolt://localhost:7687", "username", "password")) {        	
            // Setup the connection with the DB
            connect = DriverManager.getConnection("jdbc:mysql://localhost:3306?user=username&password=admin&useSSL=false&allowPublicKeyRetrieval=true");
            String databaseName = "dbname";
            String tableNm = "";
            statement = connect.createStatement();
            
            // --- LISTING DATABASE TABLE NAMES ---
            String[] types = { "TABLE" };
            resultSet = connect.getMetaData().getTables(databaseName, null, "%", types);
            while (resultSet.next()) {
            	tableNm = resultSet.getString(3);
				System.out.println("Table Name = " + tableNm);
				colResultSet = statement.executeQuery("select * from "+databaseName+"."+tableNm);
				access.writeResultSet(colResultSet, tableNm);
            }
            colResultSet.close();
            resultSet.close();
        } catch (Exception e) {
            throw e;
        } finally {
            access.close(connect, statement, resultSet);
        }

    } 
    
    private void writeResultSet(ResultSet resultSet, String tableNm) throws SQLException {
        // ResultSet is initially before the first data set
    	NeodbManager neo = new NeodbManager("bolt://localhost:7687", "username", "password");
    	List<Map<String, String>> params = null;
    	int count = 0;String node = "";
        while (resultSet.next()) {
        	node = "n1";
        	params = params == null ? new ArrayList<>() : params;
        	
        	Map<String, String> currparams = new Hashtable<String, String>();
        	
        	for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
        		String key = resultSet.getMetaData().getColumnName(i);
        		String value = resultSet.getString(key);
				currparams.put(key, value == null ?	"":value);
			}
        	
        	params.add(currparams);
        	
			count++;
			if(count%1000==0) { 
				neo.createNodes(tableNm, params); 
				params = null;
			}
        }      
        if(count<1000 && count > 1) { 
        	neo.createNodes(tableNm, params); 
        	params = null; 
        }
        
    }

    // You need to close the resultSet
    private void close(Connection connect, Statement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connect != null) {
                connect.close();
            }
        } catch (Exception e) {

        }
    }
}
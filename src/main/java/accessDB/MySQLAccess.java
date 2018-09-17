package accessDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.json.JSONObject;

public class MySQLAccess {
	
    public static void main(String[] args) throws Exception {
    	
    	Connection connect = null;
    	Statement statement = null;
    	ResultSet resultSet = null, colResultSet = null;
    	MySQLAccess access = new MySQLAccess();
        try (NeodbManager neo = new NeodbManager("bolt://localhost:7687", "neo4j", "password")) {        	
            // Setup the connection with the DB
            connect = DriverManager.getConnection("jdbc:mysql://localhost:3306?user=root&password=admin&useSSL=false&allowPublicKeyRetrieval=true");
            String databaseName = "foods";
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
    	NeodbManager neo = new NeodbManager("bolt://localhost:7687", "neo4j", "password");
    	StringBuffer msb = new StringBuffer();
    	int count = 0;String node = "";
        while (resultSet.next()) {
        	node = nameOfNode();
        	StringBuffer sb = new StringBuffer();
        	sb.append("CREATE ("+node+":"+tableNm+" {");
			for  (int i = 1; i<= resultSet.getMetaData().getColumnCount(); i++){
				JSONObject j = new JSONObject();
				j.put(resultSet.getMetaData().getColumnName(i), resultSet.getString(resultSet.getMetaData().getColumnName(i)));
				if(!j.isEmpty())
					sb.append(resultSet.getMetaData().getColumnName(i)+":\""+j.getString(resultSet.getMetaData().getColumnName(i)).replaceAll("\"", "\\\\\"")+"\",");
	        }
			sb.deleteCharAt(sb.length()-1);
			sb.append("}) ");  
			msb.append(sb.toString());
			count++;
			if(count%1000==0) { neo.createNodes(msb.toString(), node); msb = new StringBuffer(); }
			
        }      
        if(count<1000 && count > 1) { neo.createNodes(msb.toString(), node); msb = new StringBuffer(); }
        
    }
    
    public String nameOfNode() {
    	  
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 10;
        Random random = new Random();
        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int) 
              (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        return buffer.toString();
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
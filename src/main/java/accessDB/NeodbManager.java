package accessDB;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

public class NeodbManager implements AutoCloseable {
	private String uri, password, user;
	Driver driver;
	public NeodbManager( String uri, String user, String password )
    {
		this.uri = uri;
		this.user = user;
		this.password = password;
		driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }
	
	void createNodes (String node, List<Map<String, String>>params) {
		
    	try 
        { 
    		FeedNeo(driver, node, params);
        } finally {
        	
        }
    }
	
	public void FeedNeo(Driver driver, final String node, List<Map<String, String>> params )
    {
        try 
        {
        	Session session = driver.session();
        	String fullQuery = "UNWIND {rows} as batchrow ";
        	fullQuery += "CREATE (n:"+node+") SET n+= batchrow";
        	session.run(fullQuery, Collections.singletonMap("rows", params));
        	System.out.println("+1000");
        } finally {
        	
        }
    }

	@Override
	public void close() throws Exception {
		driver.close();
	}

}

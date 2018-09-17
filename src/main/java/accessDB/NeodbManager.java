package accessDB;

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
	
	void createNodes (String cyp, String node) {
		
    	try 
        { 
    		FeedNeo(driver, cyp, node);
        } finally {
        	
        }
    }
	
	public void FeedNeo(Driver driver, final String message, final String node )
    {
        try 
        {
        	Session session = driver.session();
            String greeting = session.writeTransaction(tx -> tx.run(message+" RETURN "+node+".id + ', from node ' + id("+node+")").single().get( 0 ).asString());
            System.out.println( greeting );
        } finally {
        	
        }
    }

	@Override
	public void close() throws Exception {
		driver.close();
	}

}

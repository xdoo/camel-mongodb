package org.apache.camel.processor.idempotent.test;

import java.net.UnknownHostException;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.processor.idempotent.MongoDbIdempotentRepository;
import org.apache.camel.test.CamelTestSupport;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class TestIdempotentRepositoryInRoute extends CamelTestSupport{
	
	public TestIdempotentRepositoryInRoute() throws ConfigurationException{
		Configuration config = new PropertiesConfiguration("database.properties");
		
		//set properties
		host1 = config.getString("host1");
		port1 = config.getInt("port1");
		dbName1 = config.getString("dbName1");
		collectionName1 = config.getString("collectionName1");
		
		this.prepareDB();
	}
	
	private DB db;
	private DBCollection coll;
	
	private String host1;
	private int port1;
	private String dbName1;
	private String collectionName1;

	public void setUp(){
		try {
			super.setUp();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.cleanDb();
	}
	
	public void tearDown(){
		try {
			super.tearDown();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.cleanDb();
	}
	
	public void testRepoWithId() throws InterruptedException{
		
		final MockEndpoint out 	= getMockEndpoint("mock:out1");
		
		for (int i = 0; i < 5; i++) {
			template.sendBodyAndHeader("seda:in", "foo", "myMessageId", "123");
		}
		
		out.expectedMessageCount(1);
		
		assertMockEndpointsSatisfied();
	}
	
	public void testRepoWithoutId() throws InterruptedException{

		final MockEndpoint out 	= getMockEndpoint("mock:out2");
		
		for (int i = 0; i < 5; i++) {
			template.sendBody("seda:in2", "this is a foo message");
			template.sendBody("seda:in2", "this is a FOO message");
		}
		
		out.expectedMessageCount(2);
		
		assertMockEndpointsSatisfied();
	}
	
	public void testHighLoad() throws InterruptedException{
		
		int runs = 1000;
		
		MockEndpoint out = getMockEndpoint("mock:out2");
		
		for (int i = 0; i < runs; i++) {			
			template.sendBody("seda:in2", this.getText() + i);
			template.sendBody("seda:in2", this.getText() + i);
			template.sendBody("seda:in2", this.getText() + i);
			template.sendBody("seda:in2", this.getText() + i);
			template.sendBody("seda:in2", this.getText() + i);

		}
		
		out.expectedMessageCount(runs);
		out.setResultWaitTime(60000);
		
		out.assertIsSatisfied();
	}
	
    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {

            	errorHandler(deadLetterChannel("mock:error"));
            	
        		/*
        		 * route filters out duplicated event messages
        		 * (e.g. produced from multiple server instances
        		 * in a clustered environment)  
        		 */
        		from("seda:in")
        		.routeId("mongodb_idempotent_repository")// for testing purposes it's a good idea to give every route an id 
        		.idempotentConsumer(header("myMessageId"), MongoDbIdempotentRepository.mongoIdempotentRepository(host1, port1, dbName1, collectionName1))
        		.to("mock:out1").log("body --> ${body}");

        		
        		/*
        		 * test md5
        		 */
        		from("seda:in2")
        		.routeId("mongodb_idempotent_repository_md5")
        		.idempotentConsumer(body() , MongoDbIdempotentRepository.mongoIdempotentRepositoryMd5(host1, port1, dbName1, collectionName1))
        		.to("mock:out2");
            	
            }
        };
    }
    
	private void prepareDB(){
		
		Mongo mongo;
		try {
			
			mongo = new Mongo(host1, port1);
			db = mongo.getDB(dbName1);
			coll = db.getCollection(collectionName1);

			this.cleanDb();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	private void cleanDb(){
		DBCursor cursor = coll.find();
		while (cursor.hasNext()) {
			DBObject dbObject = (DBObject) cursor.next();
			coll.remove(dbObject);
		}
	}
	
	private String getText(){
		return "this is a textblock__";
	
	}

}

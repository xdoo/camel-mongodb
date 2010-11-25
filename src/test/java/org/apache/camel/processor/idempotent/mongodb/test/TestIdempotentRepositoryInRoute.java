/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.processor.idempotent.mongodb.test;

import java.net.UnknownHostException;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.processor.idempotent.mongodb.MongoDbIdempotentRepository;
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

/**
 * 
 * @author claus.straube@catify.com
 *
 */
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
		out.setResultWaitTime(30000);
		
		out.assertIsSatisfied();
	}
	
	public void testConcurrentConsumersAndHighLoad() throws InterruptedException{
		int runs = 5000;
		
		MockEndpoint out = getMockEndpoint("mock:out3");
		
		for (int i = 0; i < runs; i++) {			
			template.sendBody("seda:in3", this.getText() + i);

		}
		
		out.expectedMessageCount(runs);
		out.setResultWaitTime(35000);
		
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
        		
        		/*
        		 * test concurrent consumers
        		 */
        		from("seda:in3")
        		.multicast().to("seda:m1", "seda:m3", "seda:m3", "seda:m4", "seda:m5");
        		
        		from("seda:m1")
        		.idempotentConsumer(body() , MongoDbIdempotentRepository.mongoIdempotentRepositoryMd5(host1, port1, dbName1, collectionName1))
        		.to("mock:out3");
        		
        		from("seda:m2")
        		.idempotentConsumer(body() , MongoDbIdempotentRepository.mongoIdempotentRepositoryMd5(host1, port1, dbName1, collectionName1))
        		.to("mock:out3");
        		
        		from("seda:m3")
        		.idempotentConsumer(body() , MongoDbIdempotentRepository.mongoIdempotentRepositoryMd5(host1, port1, dbName1, collectionName1))
        		.to("mock:out3");
        		
        		from("seda:m4")
        		.idempotentConsumer(body() , MongoDbIdempotentRepository.mongoIdempotentRepositoryMd5(host1, port1, dbName1, collectionName1))
        		.to("mock:out3");
        		
        		from("seda:m5")
        		.idempotentConsumer(body() , MongoDbIdempotentRepository.mongoIdempotentRepositoryMd5(host1, port1, dbName1, collectionName1))
        		.to("mock:out3");
            	
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
		return "this is a textblock, that will be concanated with a counter and calculated to a MD5__";
	
	}

}

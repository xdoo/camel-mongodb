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
import java.util.Date;

import org.apache.camel.processor.idempotent.mongodb.MongoDbIdempotentRepository;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

import junit.framework.TestCase;

/**
 * 
 * @author claus.straube@catify.com
 *
 */
public class TestMongoDbIdempotentRepository extends TestCase {
	
	public TestMongoDbIdempotentRepository() throws ConfigurationException{
		Configuration config = new PropertiesConfiguration("database.properties");
		
		//set properties
		host1 = config.getString("host1");
		port1 = config.getInt("port1");
		dbName1 = config.getString("dbName1");
		collectionName1 = config.getString("collectionName1");
		
		host2 = config.getString("host2");
		port2 = config.getInt("port2");
		dbName2 = config.getString("dbName2");
		collectionName2 = config.getString("collectionName2");
		
		bean = MongoDbIdempotentRepository.mongoIdempotentRepository(host1);
		this.prepareDB();
	}

	private DB db;
	private DBCollection coll;
	private IdempotentRepository<String> bean;
	
	private String host1;
	private int port1;
	private String dbName1;
	private String collectionName1;
	
	private String host2;
	private int port2;
	private String dbName2;
	private String collectionName2;

	protected void setUp() throws Exception {
		super.setUp();
		this.cleanDb();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		this.cleanDb();
	}
	
	public void testAddOne() throws InterruptedException{		
		assertEquals(0, coll.count());
		assertTrue(bean.add("123"));
		
		Thread.sleep(200);
		assertEquals(1, coll.count());
		
	}
	
	public void testAddMoreThanOne() throws InterruptedException{		
		assertEquals(0, coll.count());
		assertTrue(bean.add("123"));
		assertTrue(bean.add("456"));
		assertTrue(bean.add("789"));
		
		Thread.sleep(200);	
		assertEquals(3, coll.count());
		
	}
	
	public void testAddDuplicated() throws InterruptedException{
		
		assertEquals(0, coll.count());
		assertTrue(bean.add("123"));
		assertFalse(bean.add("123"));
		
		Thread.sleep(200);
		assertEquals(1, coll.count());
	
	}
	
	public void testConfirm() throws InterruptedException{
		
		assertEquals(0, coll.count());
		assertTrue(bean.add("123"));
		assertTrue(bean.confirm("123"));

		Thread.sleep(200);
		assertEquals(1, coll.count());
		
		DBCursor cursor = coll.find();
		assertTrue((Boolean) cursor.next().get("confirmed"));

	}
	
	public void testContains(){
		
		assertEquals(0, coll.count());
		assertTrue(bean.add("123"));
		
		assertTrue(bean.contains("123"));
		assertFalse(bean.contains("456"));

	}
	
	public void testRemoveSingle() throws InterruptedException{
		
		assertEquals(0, coll.count());
		assertTrue(bean.add("123"));

		Thread.sleep(200);
		assertEquals(1, coll.count());
		
		assertTrue(bean.remove("123"));
		
		Thread.sleep(200);
		assertEquals(0, coll.count());

		
	}
	
	public void testRemoveMulti() throws InterruptedException{
		
		assertEquals(0, coll.count());
		assertTrue(bean.add("123"));
		assertTrue(bean.add("456"));
		assertTrue(bean.add("789"));
		
		Thread.sleep(200);	
		assertEquals(3, coll.count());
		
		assertTrue(bean.remove("456"));
		
		Thread.sleep(200);	
		assertEquals(2, coll.count());

	}

	public void testMongoIdempotentRepositoryString() {
		IdempotentRepository<String> repo = MongoDbIdempotentRepository.mongoIdempotentRepository(host2);
		assertNotNull(repo);
		assertTrue(repo instanceof MongoDbIdempotentRepository);
		
		//members
		MongoDbIdempotentRepository mrepo = (MongoDbIdempotentRepository) repo;
		assertEquals(host2, mrepo.getHost());
	}

	public void testMongoIdempotentRepositoryMd5String() {
		IdempotentRepository<String> repo = MongoDbIdempotentRepository.mongoIdempotentRepository(host2);
		assertNotNull(repo);
		assertTrue(repo instanceof MongoDbIdempotentRepository);
		
		//members
		MongoDbIdempotentRepository mrepo = (MongoDbIdempotentRepository) repo;
		assertEquals(host2, mrepo.getHost());
	}

	public void testMongoIdempotentRepositoryStringInt() {
		IdempotentRepository<String> repo = MongoDbIdempotentRepository.mongoIdempotentRepository(host2, port2);
		assertNotNull(repo);
		assertTrue(repo instanceof MongoDbIdempotentRepository);
		
		//members
		MongoDbIdempotentRepository mrepo = (MongoDbIdempotentRepository) repo;
		assertEquals(host2, mrepo.getHost());
		assertEquals(port2, mrepo.getPort());
	}

	public void testMongoIdempotentRepositoryMd5StringInt() {
		IdempotentRepository<String> repo = MongoDbIdempotentRepository.mongoIdempotentRepository(host2, port2);
		assertNotNull(repo);
		assertTrue(repo instanceof MongoDbIdempotentRepository);
		
		//members
		MongoDbIdempotentRepository mrepo = (MongoDbIdempotentRepository) repo;
		assertEquals(host2, mrepo.getHost());
		assertEquals(port2, mrepo.getPort());
	}

	public void testMongoIdempotentRepositoryStringIntStringString() {
		IdempotentRepository<String> repo = MongoDbIdempotentRepository.mongoIdempotentRepository(host2, port2, dbName2, collectionName2);
		assertNotNull(repo);
		assertTrue(repo instanceof MongoDbIdempotentRepository);
		
		//members
		MongoDbIdempotentRepository mrepo = (MongoDbIdempotentRepository) repo;
		assertEquals(host2, mrepo.getHost());
		assertEquals(port2, mrepo.getPort());
		assertEquals(dbName2, mrepo.getDbName());
		assertEquals(collectionName2, mrepo.getCollectionName());
	}

	public void testMongoIdempotentRepositoryMd5StringIntStringString() {
		IdempotentRepository<String> repo = MongoDbIdempotentRepository.mongoIdempotentRepository(host2, port2, dbName2, collectionName2);
		assertNotNull(repo);
		assertTrue(repo instanceof MongoDbIdempotentRepository);
		
		//members
		MongoDbIdempotentRepository mrepo = (MongoDbIdempotentRepository) repo;
		assertEquals(host2, mrepo.getHost());
		assertEquals(port2, mrepo.getPort());
		assertEquals(dbName2, mrepo.getDbName());
		assertEquals(collectionName2, mrepo.getCollectionName());
	}
	
	public void testDeleteOldMessages() throws InterruptedException{
		
		assertEquals(0, coll.count());
		bean.add("123");
		bean.add("124");
		bean.add("125");
		bean.add("126");
		assertEquals(4, coll.count());
		
		Thread.sleep(500);
		
		bean.add("127");
		bean.add("128");
		bean.add("129");
		assertEquals(7, coll.count());
		
		MongoDbIdempotentRepository mrepo = (MongoDbIdempotentRepository) bean;
		mrepo.removeMessagesBefore(new Date().getTime() - 400);
		
		Thread.sleep(10);
		
		assertEquals(3, coll.count());
		
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
			System.out.println(dbObject);
			coll.remove(dbObject);
		}
	}

}

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
package org.apache.camel.processor.idempotent;

import java.net.UnknownHostException;
import java.util.Date;

import org.apache.camel.spi.IdempotentRepository;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;

/**
 * MongoDbIdempotentRepository implements the 
 * <a href="http://camel.apache.org/idempotent-consumer.html">Idempotent Consumer Pattern</a> (EIP) based on 
 * <a href="http://camel.apache.org">Apache Camel</a>. You can use this 
 * class in a camel route (Java DSL) as follows:
 * </br></br>
 * <code>
 *    from("seda:in")
 *    .routeId("mongodb_idempotent_repository_md5")
 *    .idempotentConsumer(body() , MongoDbIdempotentRepository.mongoIdempotentRepositoryMd5("localhost", "23000", "my_db", "idempoten_repo"))
 *    .to("seda:out");
 * </code>
 * 
 * @author claus.straube@catify.com
 * @version 2.5.0
 *
 */
public class MongoDbIdempotentRepository implements
		IdempotentRepository<String> {

	Log log = LogFactory.getLog(IdempotentRepository.class);
	
	//id or object modus
	private boolean id = true;
	
	// mongo db
	private String host = "localhost";
	private int port = 27017;
	private String dbName = "repo";
	private String collectionName = "repo";
	
	
	private DB db;
	private DBCollection coll;

	// keys in repo collection
	public final static String EVENTID = "event_id";
	public final static String CONFIRMED = "confirmed";
	public final static String TIMESTAMP = "timestamp";

	public void init() {
		try {
			Mongo mongo = new Mongo(host, port);
			db = mongo.getDB(dbName);
			coll = db.getCollection(collectionName);

			// create a unique index on event id key
			coll.ensureIndex(new BasicDBObject(EVENTID, 1), "repo_index", true);

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * adds the key to the repository
	 */
	public boolean add(String key) {

		boolean result = true;
		
		if(!this.isId()){
			key = DigestUtils.md5Hex(key);
		}
		
		log.debug("add --> " + key);

		long now = new Date().getTime();

		DBObject dbObject = new BasicDBObject();

		// we need the event id as key and a confirmation
		// field (initially set to false, because it's not
		// confirmed yet)
		dbObject.put(EVENTID, key);
		dbObject.put(TIMESTAMP, now);
		dbObject.put(CONFIRMED, false);

		WriteResult writeResult = coll.save(dbObject);

		if (writeResult.getError() != null) {
			result = false;
		}

		return result;
	}

	/**
	 * Confirms the key, after the exchange has been processed successfully.
	 */
	public boolean confirm(String key) {

		boolean result = true;
		
		if(!this.isId()){
			key = DigestUtils.md5Hex(key);
		}
		
		log.debug("confirm --> " + key);

		// object to search
		DBObject criteria = new BasicDBObject(EVENTID, key);

		// value to update
		DBObject update = new BasicDBObject("$set", new BasicDBObject(
				CONFIRMED, true));
		WriteResult writeResult = coll.update(criteria, update);

		if (writeResult.getError() != null) {

			result = false;
		}
		return result;
	}

	/**
	 * Returns true if this repository contains the specified element.
	 */
	public boolean contains(String key) {
		
		if(!this.isId()){
			key = DigestUtils.md5Hex(key);
		}
		
		log.debug("contains --> " + key);

		// object to search
		DBObject criteria = new BasicDBObject(EVENTID, key);

		// find object
		DBCursor cursor = coll.find(criteria);

		if (cursor.size() == 0) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Removes the key from the repository (usually if the exchange failed).
	 */
	public boolean remove(String key) {
		boolean result = true;
		
		if(!this.isId()){
			key = DigestUtils.md5Hex(key);
		}
		
		log.debug("remove --> " + key);

		// object to search
		DBObject criteria = new BasicDBObject(EVENTID, key);

		// value to update
		WriteResult writeResult = coll.remove(criteria);

		if (writeResult.getError() != null) {

			result = false;
		}
		return result;
	}
	
	/**
	 * Removes all messages, that are older as the given 
	 * timestamp. Use this method to avoid that your db
	 * runs full with old data...
	 * 
	 * @param timestamp
	 */
	public void removeMessagesBefore(long timestamp){
		
		log.debug("removeMessagesBefore --> " + timestamp);
		
		DBObject needle = new BasicDBObject(TIMESTAMP, new BasicDBObject("$lte", timestamp));
		coll.remove(needle);		
	}
	
	/**
	 * creates an instance of MongoIdempotentRepository with the
	 * given parameters for MongoDB. The other parameters will set to:
	 * <ul>
	 *  <li>port = 27017</li>
	 *  <li>dbName = "idempotent_repository"</li>
	 *  <li>collectionName = "repo"</li>
	 *  <li>id = true (the 'key' must be an id)</li>
	 * </ul>
	 * 
	 * @param host
	 * @return
	 */
	public static IdempotentRepository<String> mongoIdempotentRepository(String host){
		MongoDbIdempotentRepository repo = new MongoDbIdempotentRepository();
		repo.setHost(host);
		repo.init();
		return repo;
	}
	
	/**
	 * creates an instance of MongoIdempotentRepository with the
	 * given parameters for MongoDB. The other parameters will set to:
	 * <ul>
	 *  <li>port = 27017</li>
	 *  <li>dbName = "idempotent_repository"</li>
	 *  <li>collectionName = "repo"</li>
	 *  <li>id = false (a md5 hash for the string body will be calculated)</li>
	 * </ul>
	 * 
	 * @param host
	 * @return
	 */
	public static IdempotentRepository<String> mongoIdempotentRepositoryMd5(String host){
		MongoDbIdempotentRepository repo = new MongoDbIdempotentRepository();
		repo.setHost(host);
		repo.setId(false);
		repo.init();
		return repo;
	}
	
	/**
	 * creates an instance of MongoIdempotentRepository with the
	 * given parameters for MongoDB. The other parameters will set to:
	 * <ul>
	 *  <li>dbName = "idempotent_repository"</li>
	 *  <li>collectionName = "repo"</li>
	 *  <li>id = true (the 'key' must be an id)</li>
	 * </ul>
	 * 
	 * @param host
	 * @return
	 */
	public static IdempotentRepository<String> mongoIdempotentRepository(String host, int port){
		MongoDbIdempotentRepository repo = new MongoDbIdempotentRepository();
		repo.setHost(host);
		repo.setPort(port);
		repo.init();
		return repo;
	}
	
	/**
	 * creates an instance of MongoIdempotentRepository with the
	 * given parameters for MongoDB. The other parameters will set to:
	 * <ul>
	 *  <li>dbName = "idempotent_repository"</li>
	 *  <li>collectionName = "repo"</li>
	 *  <li>id = false (a md5 hash for the string body will be calculated)</li>
	 * </ul>
	 * 
	 * @param host
	 * @return
	 */
	public static IdempotentRepository<String> mongoIdempotentRepositoryMd5(String host, int port){
		MongoDbIdempotentRepository repo = new MongoDbIdempotentRepository();
		repo.setHost(host);
		repo.setPort(port);
		repo.setId(false);
		repo.init();
		return repo;
	}
	
	/**
	 * creates an instance of MongoIdempotentRepository with the
	 * given parameters for MongoDB. The other parameters will set to:
	 * <ul>
	 *  <li>id = true (the 'key' must be an id)</li>
	 * </ul>
	 * 
	 * @param host
	 * @return
	 */
	public static IdempotentRepository<String> mongoIdempotentRepository(String host, int port, String dbName, String collectionName){
		MongoDbIdempotentRepository repo = new MongoDbIdempotentRepository();
		repo.setHost(host);
		repo.setPort(port);
		repo.setDbName(dbName);
		repo.setCollectionName(collectionName);
		repo.init();
		return repo;
	}
	
	/**
	 * creates an instance of MongoIdempotentRepository with the
	 * given parameters for MongoDB. The other parameters will set to:
	 * <ul>
	 *  <li>id = false (a md5 hash for the string body will be calculated)</li>
	 * </ul>
	 * 
	 * @param host
	 * @return
	 */
	public static IdempotentRepository<String> mongoIdempotentRepositoryMd5(String host, int port, String dbName, String collectionName){
		MongoDbIdempotentRepository repo = new MongoDbIdempotentRepository();
		repo.setHost(host);
		repo.setPort(port);
		repo.setDbName(dbName);
		repo.setCollectionName(collectionName);
		repo.setId(false);
		repo.init();
		return repo;
	}
	

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public boolean isId() {
		return id;
	}

	public void setId(boolean id) {
		this.id = id;
	}

}

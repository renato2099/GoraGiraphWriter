/**
 *Licensed to the Apache Software Foundation (ASF) under one
 *or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 *regarding copyright ownership.  The ASF licenses this file
 *to you under the Apache License, Version 2.0 (the"
 *License"); you may not use this file except in compliance
 *with the License.  You may obtain a copy of the License at
 *
  * http://www.apache.org/licenses/LICENSE-2.0
 * 
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */
package org.apache.giraph.gora;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.gora.generated.Vertex;
import org.apache.giraph.gora.utils.GoraUtils;
import org.apache.giraph.gora.utils.VertexUtils;
//import org.apache.gora.dynamodb.query.DynamoDBKey;
//import org.apache.gora.dynamodb.store.DynamoDBStore;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;


/**
 * GoraRunner enables us to create a {@link DataStore} via 
 * {@link DataStoreFactory#createDataStore(Class, Class, Class, Configuration)}, then 
 * executes puts (writes first and last name, passwsord and telephone number) and gets 
 * (retrieves) the data we previously persisted.
 * We then close the data store using {@link DataStore#close()}.
 * @author renatomarroquin
 *
 */
public class GoraRunner<K, T extends PersistentBase> {
  /**
   * Data store to handle user storage
   */
  protected HashMap<String, DataStore<K, T>> dataStores;

  /**
   * Class of the key.
   */
  protected Class<K> keyClass;

  /**
   * Class of the object to be persisted.
   */
  protected Class<T> persistentClass;

  /**
   * Maximum count of vertices.
   */
  private static int MAX_VRTX_COUNT = 5000;

  /**
   * @param args
   */
  public static void main(String[] args) {

    String dsType = GoraUtils.HBASE_STORE;
    String dsName = "vertexStore";
    GoraRunner<String, Vertex> gr = new GoraRunner<String, Vertex>();
    
    // Creating data stores
    gr.addDataStore(dsName, dsType, String.class, Vertex.class);
    /**
     * [0,0,[[1,1],[3,3]]]
[1,0,[[0,1],[2,2],[3,1]]]
[2,0,[[1,2],[4,4]]]
[3,0,[[0,3],[1,1],[4,4]]]
[4,0,[[3,4],[2,4]]]
     */
    // Performing requests vertices's requests
    //gr.putRequests(dsName, VertexUtils.generateGraph(MAX_VRTX_COUNT));
    //gr.deleteRequests("simpsonStore", gr.createKey("bart.simpsone"));
    gr.verify(gr.goraRead(dsName, "1", "101"));
    System.out.println(gr.getPartitionNumber(dsName));
  }

  public int getPartitionNumber(String pDataStoreName) {
    int partitionNumber = -1;
    DataStore pDataStore = dataStores.get(pDataStoreName);
    Query<K, T> query = pDataStore.newQuery();
    query.setStartKey((K) "1");
    query.setEndKey((K) "101");
    try {
      partitionNumber = pDataStore.getPartitions(query).size();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return partitionNumber;
  }
  /**
   * Read elements from a Gorian data store using a range query.
   * @param pDataStoreName
   * @param pStartKey
   * @param pEndKey
   * @return
   */
  public Result<K, T> goraRead(String pDataStoreName, K pStartKey, K pEndKey){
    System.out.println("Performing get requests for <" + pDataStoreName + ">");
    return GoraUtils.getRequests(dataStores.get(pDataStoreName), pStartKey, pEndKey);
  }

  /**
   * Put elements into a Gorian data store.
   * @param pDataStoreName
   * @param pGraph
   */
  public void putRequests(String pDataStoreName, Map<K, T> pGraph){
    System.out.println("Performing put requests for <" + pDataStoreName + ">");
    DataStore<K, T> dataStore = dataStores.get(pDataStoreName);
    if (dataStore == null) {
      System.out.println("El data store no está");
    }
    for(K vrtxId : pGraph.keySet())
      dataStore.put(vrtxId, pGraph.get(vrtxId));
    dataStore.flush();
  }

  /**
   * Verifies if a result obtained has our vertices.
   * @param pResults
   */
  public void verify(Result<K, T> pResults){
    if (pResults != null){
      try {
        while (pResults.next()){
          Vertex vrtx = (Vertex)pResults.get();
          System.out.println(vrtx);
        }
      } catch (IOException e) {
        System.out.println("Error verifying data input.");
        e.printStackTrace();
      } catch (Exception e) {
        System.out.println("Error verifying data input.");
        e.printStackTrace();
      }
    }
    else
      System.out.println("Objetos hasn't been found.");
  }

  /**
   * Adds a data store to the ones being used.
   * @param dsName
   * @param dsType
   * @param pKeyClass
   * @param pValueClass
   */
  private void addDataStore(String dsName, String dsType, Class<K> pKeyClass, Class<T> pValueClass){
    // Setting the recently created data store into a centralized structure
    DataStore<K, T> dataStore;
    this.dataStores = new HashMap<String, DataStore<K, T>>();
    try {
      dataStore = GoraUtils.createSpecificDataStore(dsName, dsType, pKeyClass, pValueClass);
      if (dataStore != null)
        dataStores.put(dsName, dataStore);
    } catch (GoraException e) {
      System.out.println("Error adding new data store");
      e.printStackTrace();
    }
  }
  
}

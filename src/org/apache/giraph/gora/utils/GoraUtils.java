/**
 * 
 */
package org.apache.giraph.gora.utils;

import org.apache.gora.cassandra.store.CassandraStore;
import org.apache.gora.hbase.store.HBaseStore;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;

/**
 * @author renatomarroquin
 *
 */
public class GoraUtils {

  protected static Class<? extends DataStore> dataStoreClass;
  private static Configuration conf = new Configuration();
  public static final String CASSANDRA_STORE = "cassandra";
  public static final String HBASE_STORE = "hbase";

  /**
   * Creates a generic data store using the data store class set using the class property
   * @param keyClass
   * @param persistentClass
   * @return
   * @throws GoraException
   */
  @SuppressWarnings("unchecked")
  public static <K, T extends Persistent> DataStore<K,T>
    createDataStore(Class<K> keyClass, Class<T> persistentClass) throws GoraException {
    DataStoreFactory.createProps();
    DataStore<K,T> dataStore = 
        DataStoreFactory.createDataStore((Class<? extends DataStore<K,T>>)dataStoreClass, 
                                          keyClass, persistentClass,
                                          conf);

    return dataStore;
  }

  /**
   * Returns the specific type of class for the requested data store
   * @param pDataStoreName
   * @return
   */
  private static Class<? extends DataStore> getSpecificDataStore(String pDataStoreName){
    if (pDataStoreName.toLowerCase().equals(CASSANDRA_STORE)){
      return CassandraStore.class;
    }
    if (pDataStoreName.toLowerCase().equals(HBASE_STORE)){
      return HBaseStore.class;
    }
    //if (pDataStoreName == "DynamoDB"){
      //  return DynamoDBStore.class;
    //}
    return null;
  }

  public static <K, T extends Persistent> DataStore<K,T>
  createSpecificDataStore(String pDataStoreName, String pDataStoreType, Class<K> keyClass, Class<T> persistentClass) throws GoraException {
   // Getting the specific data store
   dataStoreClass = getSpecificDataStore(pDataStoreType);
   return createDataStore(keyClass, persistentClass);
 }

  public static <K, T extends Persistent> Result<K, T>
    getRequests(DataStore<K,T> pDataStore, K pStartKey, K pEndKey){
    Query<K, T> query = pDataStore.newQuery();
    query.setStartKey(pStartKey);
    query.setEndKey(pEndKey);
    return pDataStore.execute(query);
  }
}

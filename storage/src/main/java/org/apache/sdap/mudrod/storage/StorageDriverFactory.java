/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sdap.mudrod.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.sdap.mudrod.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class StorageDriverFactory {

  public static final Logger log = LoggerFactory.getLogger(StorageDriverFactory.class);

  public static final String MUDROD_DEFAULT_PROPERTIES_FILE = "config.properties";

  public static final String MUDROD_DEFAULT_DATASTORE_KEY = "mudrod.datastore.default";

  private StorageDriverFactory() { }

  /**
   * Creates a new {@link Properties}. It adds the default gora configuration
   * resources. This properties object can be modified and used to instantiate
   * store instances. It is recommended to use a properties object for a single
   * store, because the properties object is passed on to store initialization
   * methods that are able to store the properties as a field.   
   * @return The new properties object.
   */
  public static Properties createProps() {
    try {
      Properties properties = new Properties();
      InputStream stream = StorageDriverFactory.class.getClassLoader()
              .getResourceAsStream(MUDROD_DEFAULT_PROPERTIES_FILE);
      if(stream != null) {
        try {
          properties.load(stream);
          return properties;
        } finally {
          stream.close();
        }
      } else {
        log.warn(MUDROD_DEFAULT_PROPERTIES_FILE + " not found, properties will be empty.");
      }
      return properties;
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void initializeStorageDriver(
          StorageDriver storageDriver, Properties properties) throws IOException {
    storageDriver.initialize(properties);
  }

  /**
   * Instantiate a new {@link DataStore}.
   * 
   * @param storageDriverClass The datastore implementation class.
   * @param properties The properties to be used in the store.
   * @return A new {@link org.apache.sdap.mudrod.storage.StorageDriver} instance.
   * @throws Exception
   */
  public static StorageDriver createDataStore(Class<?> storageDriverClass, Properties properties) throws Exception{
    try {
      StorageDriver storageDriver =
              (StorageDriver) ReflectionUtils.newInstance(storageDriverClass);
      initializeStorageDriver(storageDriver, properties);
      return storageDriver;
    } catch(Exception ex) {
      throw new Exception(ex);
    }
  }
}

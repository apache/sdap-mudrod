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
package org.apache.sdap.mudrod.storage.solr;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.sdap.mudrod.storage.StorageDriver;

/**
 *
 */
public class SolrDriver implements StorageDriver {

  /**
   * @param props 
   * 
   */
  public SolrDriver(Properties props) {
    // TODO Auto-generated constructor stub
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#createBulkProcessor()
   */
  @Override
  public void createBulkProcessor() {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#destroyBulkProcessor()
   */
  @Override
  public void destroyBulkProcessor() {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#putMapping(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void putMapping(String indexName, String settingsJson, String mappingJson) throws IOException {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#customAnalyzing(java.lang.String, java.lang.String)
   */
  @Override
  public String customAnalyzing(String indexName, String str) throws InterruptedException, ExecutionException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#customAnalyzing(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public String customAnalyzing(String indexName, String analyzer, String str) throws InterruptedException, ExecutionException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#customAnalyzing(java.lang.String, java.util.List)
   */
  @Override
  public List<String> customAnalyzing(String indexName, List<String> list) throws InterruptedException, ExecutionException {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#deleteType(java.lang.String, java.lang.String)
   */
  @Override
  public void deleteType(String index, String type) {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#getTypeListWithPrefix(java.lang.Object, java.lang.Object)
   */
  @Override
  public List<String> getTypeListWithPrefix(Object object, Object object2) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#getIndexListWithPrefix(java.lang.Object)
   */
  @Override
  public List<String> getIndexListWithPrefix(Object object) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#searchByQuery(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public String searchByQuery(String index, String type, String query) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#searchByQuery(java.lang.String, java.lang.String, java.lang.String, java.lang.Boolean)
   */
  @Override
  public String searchByQuery(String index, String type, String query, Boolean bDetail) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#buildMeasurementHierarchies(java.util.List, java.util.List, java.util.List, java.util.List)
   */
  @Override
  public List<List<String>> buildMeasurementHierarchies(List<String> topics, List<String> terms, List<String> variables, List<String> variableDetails) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#autoComplete(java.lang.String, java.lang.String)
   */
  @Override
  public List<String> autoComplete(String index, String term) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#close()
   */
  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#refreshIndex()
   */
  @Override
  public void refreshIndex() {
    // TODO Auto-generated method stub

  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#getDocCount(java.lang.String, java.lang.String[])
   */
  @Override
  public int getDocCount(String index, String... type) {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see org.apache.sdap.mudrod.storage.StorageDriver#getDocCount(java.lang.String[], java.lang.String[])
   */
  @Override
  public int getDocCount(String[] index, String[] type) {
    // TODO Auto-generated method stub
    return 0;
  }

}

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
package org.apache.sdap.mudrod.discoveryengine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.AbstractElasticsearchIntegrationTest;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WeblogDiscoveryEngineTest extends AbstractElasticsearchIntegrationTest {

  private static WeblogDiscoveryEngine weblogEngine = null;
  
  static String DIR_TESTDATA_ONE = "Testing_Data_1_3dayLog+Meta+Onto";

  @BeforeClass
  public static void setUp() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    Properties props = mudrodEngine.loadConfig();
    ESDriver es = new ESDriver(props);
    SparkDriver spark = new SparkDriver(props);
    String dataDir = getTestDataPath(DIR_TESTDATA_ONE);
    System.out.println(dataDir);
    props.setProperty(MudrodConstants.DATA_DIR, dataDir);
    MudrodEngine.loadPathConfig(mudrodEngine, dataDir);
    weblogEngine = new WeblogDiscoveryEngine(props, es, spark);
  }

  @AfterClass
  public static void tearDown() {
  }

  private static String getTestDataPath(String testDataDir) {
    String path = WeblogDiscoveryEngineTest.class.getClassLoader().getResource(testDataDir).toString();
    if(path.startsWith("file:/")){
      path = path.replaceAll("file:/", "");
    }
    return path;
  }

  @Test
  public void testPreprocess() throws IOException {
    weblogEngine.preprocess();
    testPreprocessUserHistory();
    testPreprocessClickStream();
  }

  private void testPreprocessUserHistory() throws IOException {
    // compare user history data
    String userHistorycsvFile = getTestDataPath(DIR_TESTDATA_ONE) + "/userHistoryMatrix.csv";
    System.out.println(userHistorycsvFile);
    HashMap<String, List<String>> map = extractPairFromCSV(userHistorycsvFile);
    Assert.assertEquals("failed in history data result!", "195.219.98.7", String.join(",", map.get("sea surface topography")));
  }

  private void testPreprocessClickStream() throws IOException {
    String clickStreamcsvFile = getTestDataPath(DIR_TESTDATA_ONE) + "/clickStreamMatrix.csv";
    System.out.println(clickStreamcsvFile);
    HashMap<String, List<String>> map = extractPairFromCSV(clickStreamcsvFile);
    System.out.println(map);
    Assert.assertEquals("failed in click stream result!", "\"ostm_l2_ost_ogdr_gps\"", String.join(",", map.get("sea surface topography")));
  }
  
  private HashMap extractPairFromCSV(String csvfile){
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(csvfile));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    String line = null;
    HashMap<String, List<String>> map = new HashMap<>();
    int i = 0;
    List<String> header = new LinkedList<>();
    try {
      while ((line = br.readLine()) != null) {
        if (i == 0) {
          String str[] = line.split(",");
          for (String s : str) {
            header.add(s);
          }
        } else {
          String str[] = line.split(",");
          for (int j = 1; j < str.length; j++) {
            double value = Double.parseDouble(str[j]);
            if (value > 0.0) {
              if (!map.containsKey(str[0])) {
                map.put(str[0], new ArrayList<>());
              }
              map.get(str[0]).add(header.get(j));
            }
          }
        }
        i += 1;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return map;
  }
}

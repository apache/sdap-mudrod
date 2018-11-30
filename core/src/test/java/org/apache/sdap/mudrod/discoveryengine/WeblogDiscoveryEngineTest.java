package org.apache.sdap.mudrod.discoveryengine;

import static org.junit.Assert.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
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

  @BeforeClass
  public static void setUp() {
    MudrodEngine mudrodEngine = new MudrodEngine();
    Properties props = mudrodEngine.loadConfig();
    ESDriver es = new ESDriver(props);
    SparkDriver spark = new SparkDriver(props);
    String dataDir = getTestDataPath();
    System.out.println(dataDir);
    props.setProperty(MudrodConstants.DATA_DIR, dataDir);
    MudrodEngine.loadPathConfig(mudrodEngine, dataDir);
    weblogEngine = new WeblogDiscoveryEngine(props, es, spark);
  }

  @AfterClass
  public static void tearDown() {
    // TODO
  }

  private static String getTestDataPath() {
    File resourcesDirectory = new File("src/test/resources/");
    String resourcedir = "/Testing_Data_1_3dayLog+Meta+Onto/";
    String dataDir = resourcesDirectory.getAbsolutePath() + resourcedir;
    return dataDir;
  }

  @Test
  public void testPreprocess() throws IOException {

    weblogEngine.preprocess();
    testPreprocess_userHistory();
    testPreprocess_clickStream();
  }

  private void testPreprocess_userHistory() throws IOException {
    // compare user history data
    String userHistorycsvFile = getTestDataPath() + "/userHistoryMatrix.csv";
    BufferedReader br = new BufferedReader(new FileReader(userHistorycsvFile));
    String line = null;
    HashMap<String, List<String>> map = new HashMap<>();
    int i = 0;
    List<String> header = new LinkedList<>();
    while ((line = br.readLine()) != null) {
      if (i == 0) {
        String str[] = line.split(",");
        for (String s : str) {
          header.add(s);
        }
      } else {
        String str[] = line.split(",");
        for (int j = 1; j < str.length; j++) {
          if (!str[j].equals("0")) {
            if (!map.containsKey(str[0])) {
              map.put(str[0], new ArrayList<>());
            }
            map.get(str[0]).add(header.get(j));
          }
        }
      }
      i += 1;
    }

    Assert.assertEquals("failed in history data result!", "195.219.98.7", String.join(",", map.get("sea surface topography")));
  }

  private void testPreprocess_clickStream() throws IOException {
    // TODO compare clickStream data
    // String clickStreamcsvFile =
    // "C:/Users/admin/Documents/GitHub/incubator-sdap-mudrod/core/clickStreamMatrix.csv";
    String clickStreamcsvFile = getTestDataPath() + "/clickStreamMatrix.csv";
    System.out.println(clickStreamcsvFile);
    BufferedReader br = new BufferedReader(new FileReader(clickStreamcsvFile));
    String line = null;
    HashMap<String, List<String>> map = new HashMap<>();

    int i = 0;
    List<String> header = new LinkedList<>();
    while ((line = br.readLine()) != null) {
      if (i == 0) {
        String str[] = line.split(",");
        for (String s : str) {
          header.add(s);
        }
      } else {
        String str[] = line.split(",");
        for (int j = 1; j < str.length; j++) {
          if (!str[j].equals("0.0")) { //
            if (!map.containsKey(str[0])) {
              map.put(str[0], new ArrayList<>());
            }
            map.get(str[0]).add(header.get(j));
          }
        }
      }
      i += 1;
    }
    System.out.println(map);

    Assert.assertEquals("failed in click stream result!", "\"ostm_l2_ost_ogdr_gps\"", String.join(",", map.get("sea surface topography")));
  }
}

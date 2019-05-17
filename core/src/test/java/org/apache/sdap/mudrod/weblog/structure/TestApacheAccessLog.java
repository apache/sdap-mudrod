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
package org.apache.sdap.mudrod.weblog.structure;

import org.apache.sdap.mudrod.weblog.structure.log.ApacheAccessLog;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.ParseException;
import java.util.Properties;

import static org.junit.Assert.assertNotEquals;

public class TestApacheAccessLog {

  private static Properties testProperties = new Properties();

  @BeforeClass
  public static void loadProperties() throws IOException {

    URL configURL = ClassLoader.getSystemClassLoader().getResource("config.properties");

    assert configURL != null : "Could not load config.properties";
    try (InputStream instream = new FileInputStream(configURL.getFile())) {
      testProperties.load(instream);
    }
  }

  @Test
  public void testLogMatch() throws IOException, ParseException {
    
    // only have 7 fields? Should be 9? why not equal?
//    String testLogLine = "198.118.243.84 - - [31/Dec/2017:23:59:20 +0000] \"GET /events?page=12&amp%25252525252525252525252525252525252525253Bsort=asc&order=field_location&sort=desc HTTP/1.1\" 200 86173";
    String testLogLine =  "140.90.107.54 - - [01/Feb/2015:17:08:09 -0800] \"GET /datasetlist?search=VIIRS HTTP/1.1\" 200 82024 \"http://podaac.jpl.nasa.gov/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0\"";
    String result = ApacheAccessLog.parseFromLogLine(testLogLine, testProperties);

    assertNotEquals("Log line does not match", "{}", result);
  }
}

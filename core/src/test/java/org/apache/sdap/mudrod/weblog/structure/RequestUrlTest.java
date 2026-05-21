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

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.sdap.mudrod.weblog.structure.log.RequestUrl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RequestUrlTest {
  
  String strURL = "https://podaac.jpl.nasa.gov/datasetlist?ids=Collections:Measurement:SpatialCoverage:Platform:Sensor&values=SCAT_BYU_L3_OW_SIGMA0_ENHANCED:Sea%20Ice:Bering%20Sea:ERS-2:AMI&view=list";

  @Test
  public void testUrlPage() {
//    RequestUrl url = new RequestUrl();
    String result = RequestUrl.urlPage(strURL);
    Assert.assertEquals("You did not pass urlPage function ", "https://podaac.jpl.nasa.gov/datasetlist", result);
  }

  @Test
  public void testuRLRequest() {
//    RequestUrl url = new RequestUrl();
    Map<String, String> result = RequestUrl.uRLRequest(strURL);
    Assert.assertEquals("You did not pass uRLRequest function!", "list", result.get("view"));
    Assert.assertEquals("You did not pass uRLRequest function!", "scat_byu_l3_ow_sigma0_enhanced:sea%20ice:bering%20sea:ers-2:ami", result.get("values"));
    Assert.assertEquals("You did not pass uRLRequest function!", "collections:measurement:spatialcoverage:platform:sensor", result.get("ids"));
  }

  @Test
  public void testGetSearchInfo() {
    RequestUrl url = new RequestUrl();
    String result = null;
    try {
      result = url.getSearchInfo(strURL);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    Assert.assertEquals("You did not pass GetSearchInfo function ", "scat byu l3 ow sigma0 enhanced,sea ice,bering sea,ers 2,ami", result);
  }

  @Test
  public void testGetFilterInfo() {
    RequestUrl url = new RequestUrl();
    try {
      Map<String, String> result = url.getFilterInfo(strURL);
      Assert.assertEquals("You did not pass GetFilterInfo function!", "scat byu l3 ow sigma0 enhanced", result.get("collections"));
      Assert.assertEquals("You did not pass GetFilterInfo function!", "bering sea", result.get("spatialcoverage"));
      Assert.assertEquals("You did not pass GetFilterInfo function!", "ami", result.get("sensor"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }
}

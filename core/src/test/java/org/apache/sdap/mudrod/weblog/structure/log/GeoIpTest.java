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
package org.apache.sdap.mudrod.weblog.structure.log;

import org.junit.Assert;
import org.junit.Test;

public class GeoIpTest {

  @Test
  public void testToLocation() {
    GeoIp ip = new GeoIp();
    String iptest = "185.10.104.194";
    
    Coordinates result = ip.toLocation(iptest);
    Assert.assertEquals("failed in geoip function!", "22.283001,114.150002", result.latlon);
  }
}

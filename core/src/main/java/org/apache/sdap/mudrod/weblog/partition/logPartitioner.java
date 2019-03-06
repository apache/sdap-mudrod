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
package org.apache.sdap.mudrod.weblog.partition;

import org.apache.spark.Partitioner;

import java.util.Map;

public class logPartitioner extends Partitioner {

  private int num;
  private Map<String, Integer> UserGroups;

  public logPartitioner(int num) {
    this.num = num;
  }

  public logPartitioner(Map<String, Integer> UserGroups, int num) {
    this.UserGroups = UserGroups;
    this.num = num;
  }

  @Override
  public int getPartition(Object arg0) {
    // TODO Auto-generated method stub
    String user = (String) arg0;
    return UserGroups.get(user);
  }

  @Override
  public int numPartitions() {
    // TODO Auto-generated method stub
    return num;
  }
}

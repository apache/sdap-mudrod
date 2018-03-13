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
package org.apache.sdap.mudrod.metadata.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.sdap.mudrod.driver.ESDriver;

public abstract class Metadata implements Serializable  {

  private static final long serialVersionUID = 1L;
  protected String shortname;
  
  public Metadata() {
  }

  public Metadata(String shortname) {
    this.shortname = shortname;
  }

  /**
   * getShortName:get short name of data set
   *
   * @return data set short name
   */
  public String getShortName() {
    return this.shortname;
  }

  /**
   * getAbstract:get abstract of data set
   *
   * @return data set abstract
   */
  public abstract List<String> getAllTermList();
}

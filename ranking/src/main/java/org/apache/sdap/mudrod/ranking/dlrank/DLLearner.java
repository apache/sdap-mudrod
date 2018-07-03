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
package org.apache.sdap.mudrod.ranking.dlrank;

import java.io.File;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.ranking.common.Learner;
import org.apache.sdap.mudrod.ranking.common.LearnerFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class DLLearner extends Learner {
  private static final long serialVersionUID = 1L;
  transient SparkContext sc = null;

  public DLLearner(Properties props, ESDriver es, SparkDriver spark, String dlModel) {
    super(props, es, spark);

    sc = spark.sc.sc();
    load(dlModel);
  }

  @Override
  public void train(String trainFile) {

  }

  @Override
  public double predict(double[] value) {
    return 0;
  }

  @Override
  public void save() {
  }

  @Override
  public void load(String svmSgdModel) {
  }

  @Override
  public String customizeTrainData(String sourceDir) {
    return null;
  }
}

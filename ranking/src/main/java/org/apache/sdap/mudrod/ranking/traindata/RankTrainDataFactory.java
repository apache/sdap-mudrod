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
package org.apache.sdap.mudrod.ranking.traindata;

import org.apache.sdap.mudrod.discoveryengine.MudrodAbstract;
import org.apache.sdap.mudrod.discoveryengine.WeblogDiscoveryEngine;
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.ranking.ranksvm.RankSVMLearner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.File;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

/**
 * Create train data from difference sources, including experts provided data,
 * offline logs and realtime logs
 */
public class RankTrainDataFactory extends MudrodAbstract {

  public RankTrainDataFactory(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  public String createRankTrainData(String source, String sourceDir) {

    String result = "";
    switch (source) {
      
      case "experts":
        result = createRankTrainDataFromExperts(sourceDir);
        break;
      case "index":
        result = createRankTrainDataFromIndices(sourceDir);
        break;
      case "log":
        result = createRankTrainDataFromLog(sourceDir);
        break;
    }
    return result;
  }

  public String createRankTrainDataFromExperts(String sourceDir) {
    File sourceFile = new File(sourceDir);
    boolean bDir = sourceFile.isDirectory();
    boolean multFiles = false;
    if (bDir) {
      multFiles = true;
    }

    String separator = System.getProperty("file.separator");
    String resultDir = sourceFile.getParent() + separator + "experts_trainsets.csv";
    ExpertRankTrainData converter = new ExpertRankTrainData(sourceDir, resultDir, true);
    converter.convertToTrainSet();

    return resultDir;
  }

  public String createRankTrainDataFromIndices(String indexNames) {

    if (indexNames == "") {
      List<String> logIndexList = es.getIndexListWithPrefix(props.getProperty(MudrodConstants.LOG_INDEX));
      indexNames = String.join(",", logIndexList);
    }

    LogRankTrainDataExtractor extractor = new LogRankTrainDataExtractor(props, es, spark);
    JavaRDD<LogRankTrainData> trainDataRDD = null;
    try {
      trainDataRDD = extractor.extractTrainDataFromIndices(indexNames);
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    String trainSetFile = "E:\\Mudrod_input_data\\Testing_Data_4_1monthLog+Meta+Onto\\traing.txt";
    JavaRDD<String> jsonRDD = trainDataRDD.map(f -> f.toJson());
    jsonRDD.coalesce(1, true).saveAsTextFile(trainSetFile);

    return trainSetFile;
  }

  public String createRankTrainDataFromLog(String logDir) {

    WeblogDiscoveryEngine webEngine = new WeblogDiscoveryEngine(props, es, spark);
    String indices = webEngine.logIngestAndParse(logDir);

    String trainSetFile = createRankTrainDataFromIndices(indices);
    return trainSetFile;
  }
}

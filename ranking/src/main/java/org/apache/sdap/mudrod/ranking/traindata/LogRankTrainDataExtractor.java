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
import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.weblog.structure.log.RequestUrl;
import org.apache.sdap.mudrod.weblog.structure.session.ClickStream;
import org.apache.sdap.mudrod.weblog.structure.session.Session;
import org.apache.sdap.mudrod.weblog.structure.session.SessionExtractor;
import org.apache.sdap.mudrod.weblog.structure.session.SessionNode;
import org.apache.sdap.mudrod.weblog.structure.session.SessionTree;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * ClassName: SessionExtractor Function: Extract sessions details from
 * reconstructed sessions.
 */
public class LogRankTrainDataExtractor extends MudrodAbstract {

  public LogRankTrainDataExtractor(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
  }

  /**
   * extractTrainDataFromIndex: extract rank train data from logs stored in Elasticsearch.
   *
   * @param logIndices
   *          the index name
   * @return LogRankTrainData list {@link LogRankTrainData}
   */
  public JavaRDD<LogRankTrainData> extractTrainDataFromIndices(String logIndices) throws UnsupportedEncodingException {
    List<LogRankTrainData> result = new ArrayList<>();
    SessionExtractor sessionExtrator = new SessionExtractor();
    List<String> sessions = sessionExtrator.getSessions(props, es, logIndices);

    JavaRDD<String> sessionRDD = spark.sc.parallelize(sessions, 16);
    JavaRDD<LogRankTrainData> trainDataRDD = sessionRDD.mapPartitions(new FlatMapFunction<Iterator<String>, LogRankTrainData>() {
      private static final long serialVersionUID = 1L;
      @Override
      public Iterator<LogRankTrainData> call(Iterator<String> arg0) throws Exception {
        ESDriver tmpES = new ESDriver(props);
        tmpES.createBulkProcessor();
        List<LogRankTrainData> trainData = new ArrayList<>();
        while (arg0.hasNext()) {
          String s = arg0.next();
          String[] sArr = s.split(",");
          List<LogRankTrainData> data = extractTrainDataFromSession(sArr[1], sArr[2], sArr[0]);
          trainData.addAll(data);
        }
        tmpES.destroyBulkProcessor();
        tmpES.close();
        return trainData.iterator();
      }
    });

    return trainDataRDD;
  }

  /**
   * Obtain the ranking training data.
   *
   * @param indexName
   *          the index from whcih to obtain the data
   * @return {@link ClickStream}
   * @throws UnsupportedEncodingException
   *           if there is an error whilst processing the ranking training data.
   */
  public List<LogRankTrainData> extractTrainDataFromSession(String indexName, String cleanuptype, String sessionID) throws UnsupportedEncodingException {

    List<LogRankTrainData> trainDatas = new ArrayList<>();

    Session session = new Session(props, es);
    SessionTree tree = null;
    tree = session.getSessionTree(indexName, cleanuptype, sessionID);

    List<SessionNode> queryNodes = tree.getQueryNodes(tree.getRoot());
    for (SessionNode querynode : queryNodes) {
      List<SessionNode> children = querynode.getChildren();

      LinkedHashMap<String, Boolean> datasetOpt = new LinkedHashMap<>();
      int ndownload = 0;
      for (SessionNode node : children) {
        if ("dataset".equals(node.getKey())) {
          Boolean bDownload = false;
          List<SessionNode> nodeChildren = node.getChildren();
          for (SessionNode aNodeChildren : nodeChildren) {
            if ("ftp".equals(aNodeChildren.getKey())) {
              bDownload = true;
              ndownload += 1;
              break;
            }
          }
          datasetOpt.put(node.getDatasetId(), bDownload);
        }
      }

      // method 1: The priority of download data are higher
      if (datasetOpt.size() > 1 && ndownload > 0) {
        // query
        RequestUrl requestURL = new RequestUrl();
        String queryUrl = querynode.getRequest();
        String infoStr = requestURL.getSearchInfo(queryUrl);
        String query = null;
        try {
          query = es.customAnalyzing(props.getProperty(MudrodConstants.ES_INDEX_NAME), infoStr);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("Error performing custom analyzing", e);
        }
        Map<String, String> filter = RequestUrl.getFilterInfo(queryUrl);

        for (String datasetA : datasetOpt.keySet()) {
          Boolean bDownloadA = datasetOpt.get(datasetA);
          if (bDownloadA) {
            for (String datasetB : datasetOpt.keySet()) {
              Boolean bDownloadB = datasetOpt.get(datasetB);
              if (!bDownloadB) {

                String[] queries = query.split(",");
                for (String query1 : queries) {
                  LogRankTrainData trainData = new LogRankTrainData(query1, datasetA, datasetB);
                  trainData.setSessionId(tree.getSessionId());
                  trainData.setIndex(indexName);
                  trainData.setFilter(filter);
                  trainDatas.add(trainData);
                }
              }
            }
          }
        }
      }
    }

    return trainDatas;
  }
}

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
package org.apache.sdap.mudrod.weblog.pre;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.weblog.structure.log.RequestUrl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to post-process session, including summarizing statistics
 * and filtering
 */
public class SessionStatistic extends LogAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SessionStatistic.class);

  public SessionStatistic(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Session Summarization.");
    startTime = System.currentTimeMillis();
    try {
      processSession();
    } catch (IOException | ExecutionException e) {
      LOG.error("Session summarization process failed.", e);
    } catch (InterruptedException e) {
      LOG.error("Current running thread interrupted.", e);
      Thread.currentThread().interrupt();
    }
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("Session Summarization complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
    return null;
  }

  private void processSession() throws InterruptedException, IOException, ExecutionException {
    processSessionInParallel();
  }

  /**
   * Extract the dataset ID from a long request
   *
   * @param request raw log request
   * @return dataset ID
   */
  private String findDataset(String request) {
    String pattern1 = props.getProperty(MudrodConstants.VIEW_MARKER);
    String pattern2;
    if (request.contains("?")) {
      pattern2 = "?";
    } else {
      pattern2 = " ";
    }

    Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
    Matcher m = p.matcher(request);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  private void processSessionInParallel() throws InterruptedException, IOException {

    List<String> sessions = this.getSessions();
    JavaRDD<String> sessionRDD = spark.sc.parallelize(sessions, partition);

    int sessionCount = 0;
    sessionCount = sessionRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
      @Override
      public Iterator<Integer> call(Iterator<String> arg0) throws Exception {
        ESDriver tmpES = new ESDriver(props);
        tmpES.createBulkProcessor();
        List<Integer> sessionNums = new ArrayList<>();
        sessionNums.add(0);
        while (arg0.hasNext()) {
          String s = arg0.next();
          Integer sessionNum = processSession(tmpES, s);
          sessionNums.add(sessionNum);
        }
        tmpES.destroyBulkProcessor();
        tmpES.close();
        return sessionNums.iterator();
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });

    LOG.info("Final Session count: {}", Integer.toString(sessionCount));
  }

  private int processSession(ESDriver es, String sessionId) throws IOException, InterruptedException, ExecutionException {

    String inputType = cleanupType;
    String outputType = sessionStats;

    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    String min = null;
    String max = null;
    DateTime start = null;
    DateTime end = null;
    int duration = 0;
    float requestRate = 0;

    int sessionCount = 0;
    Pattern pattern = Pattern.compile("get (.*?) http/*");

    StatsAggregationBuilder statsAgg = AggregationBuilders.stats("Stats").field("Time");

    BoolQueryBuilder filterSearch = new BoolQueryBuilder();
    filterSearch.must(QueryBuilders.termQuery("SessionID", sessionId));

    SearchResponse sr = es.getClient()
            .prepareSearch(logIndex)
            .setTypes(inputType)
            .setQuery(filterSearch)
            .addAggregation(statsAgg)
            .execute()
            .actionGet();

    Stats agg = sr.getAggregations().get("Stats");
    min = agg.getMinAsString();
    max = agg.getMaxAsString();
    start = fmt.parseDateTime(min);
    end = fmt.parseDateTime(max);

    duration = Seconds.secondsBetween(start, end).getSeconds();

    int searchDataListRequestCount = 0;
    int searchDataRequestCount = 0;
    int searchDataListRequestByKeywordsCount = 0;
    int ftpRequestCount = 0;
    int keywordsNum = 0;

    String iP = null;
    String keywords = "";
    String views = "";
    String downloads = "";

    SearchResponse scrollResp = es.getClient()
            .prepareSearch(logIndex)
            .setTypes(inputType)
            .setScroll(new TimeValue(60000))
            .setQuery(filterSearch)
            .setSize(100)
            .execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();

        String request = (String) result.get("Request");
        String logType = (String) result.get("LogType");
        iP = (String) result.get("IP");
        Matcher matcher = pattern.matcher(request.trim().toLowerCase());
        while (matcher.find()) {
          request = matcher.group(1);
        }

        String datasetlist = props.getProperty(MudrodConstants.SEARCH_MARKER);
        String dataset = props.getProperty(MudrodConstants.VIEW_MARKER);
        if (request.contains(datasetlist)) {
          searchDataListRequestCount++;

          RequestUrl requestURL = new RequestUrl();
          String infoStr = requestURL.getSearchInfo(request) + ",";
          String info = es.customAnalyzing(props.getProperty(MudrodConstants.ES_INDEX_NAME), infoStr);

          if (!",".equals(info)) {
            if ("".equals(keywords)) {
              keywords = keywords + info;
            } else {
              String[] items = info.split(",");
              String[] keywordList = keywords.split(",");
              for (String item : items) {
                if (!Arrays.asList(keywordList).contains(item)) {
                  keywords = keywords + item + ",";
                }
              }
            }
          }

        }
        if (request.startsWith(dataset)) {
          searchDataRequestCount++;
          if (findDataset(request) != null) {
            String view = findDataset(request);
            if ("".equals(views)) 
              views = view;
            else if (!views.contains(view)) 
              views = views + "," + view;
          }
        }
        if (MudrodConstants.FTP_LOG.equals(logType)) {
          ftpRequestCount++;
          String download = "";
          String requestLowercase = request.toLowerCase();
          if (!requestLowercase.endsWith(".jpg") && 
                  !requestLowercase.endsWith(".pdf") && 
                  !requestLowercase.endsWith(".txt") && 
                  !requestLowercase.endsWith(".gif")) {
            download = request;
          }

          if ("".equals(downloads)) {
            downloads = download;
          } else {
            if (!downloads.contains(download)) {
              downloads = downloads + "," + download;
            }
          }
        }

      }

      scrollResp = es.getClient()
              .prepareSearchScroll(scrollResp.getScrollId())
              .setScroll(new TimeValue(600000))
              .execute()
              .actionGet();
      // Break condition: No hits are returned
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    if (!"".equals(keywords)) {
      keywordsNum = keywords.split(",").length;
    }

    if (searchDataListRequestCount != 0 && 
            searchDataListRequestCount <= Integer.parseInt(props.getProperty(MudrodConstants.SEARCH_F)) && 
            searchDataRequestCount != 0 && 
            searchDataRequestCount <= Integer.parseInt(props.getProperty(MudrodConstants.VIEW_F)) && 
            ftpRequestCount <= Integer.parseInt(props.getProperty(MudrodConstants.DOWNLOAD_F))) 
    {
      String sessionURL = props.getProperty(
              MudrodConstants.SESSION_PORT)
              + props.getProperty(MudrodConstants.SESSION_URL)
              + "?sessionid=" + sessionId
              + "&sessionType=" + outputType
              + "&requestType=" + inputType;
      sessionCount = 1;

      IndexRequest ir = new IndexRequest(logIndex, outputType).source(
              jsonBuilder().startObject()
              .field("SessionID", sessionId)
              .field("SessionURL", sessionURL)
              .field("Duration", duration)
              .field("Number of Keywords", keywordsNum)
              .field("Time", min)
              .field("End_time", max)
              .field("searchDataListRequest_count", searchDataListRequestCount)
              .field("searchDataListRequest_byKeywords_count", searchDataListRequestByKeywordsCount)
              .field("searchDataRequest_count", searchDataRequestCount)
              .field("keywords", es.customAnalyzing(logIndex, keywords))
              .field("views", views)
              .field("downloads", downloads)
              .field("request_rate", requestRate)
              .field("Comments", "")
              .field("Validation", 0)
              .field("Produceby", 0)
              .field("Correlation", 0)
              .field("IP", iP).endObject());

      es.getBulkProcessor().add(ir);
    }

    return sessionCount;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}

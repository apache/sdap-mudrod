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
import org.apache.sdap.mudrod.weblog.structure.log.ApacheAccessLog;
import org.apache.sdap.mudrod.weblog.structure.log.FtpLog;
import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to parse and process FTP and HTTP log files
 */
public class ImportLogFile extends LogAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(ImportLogFile.class);

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] " + "\"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";

  public static final int NUM_FIELDS = 9;
  Pattern p = Pattern.compile(logEntryPattern);
  transient Matcher matcher;

  /**
   * Constructor supporting a number of parameters documented below.
   *
   * @param props a {@link java.util.Map} containing K,V of type String, String
   *              respectively.
   * @param es    the {@link ESDriver} used to persist log
   *              files.
   * @param spark the {@link SparkDriver} used to process
   *              input log files.
   */
  public ImportLogFile(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Log Import {}", props.getProperty(MudrodConstants.TIME_SUFFIX));
    startTime = System.currentTimeMillis();
    readFile();
    endTime = System.currentTimeMillis();
    LOG.info("Log Import complete. Time elapsed {} seconds", (endTime - startTime) / 1000);
    es.refreshIndex();
    return null;
  }

  /**
   * Utility function to aid String to Number formatting such that three letter
   * months such as 'Jan' are converted to the Gregorian integer equivalent.
   *
   * @param time the input {@link java.lang.String} to convert to int.
   * @return the converted Month as an int.
   */
  public String switchtoNum(String time) {
    String newTime = time;
    if (newTime.contains("Jan")) {
      newTime = newTime.replace("Jan", "1");
    } else if (newTime.contains("Feb")) {
      newTime = newTime.replace("Feb", "2");
    } else if (newTime.contains("Mar")) {
      newTime = newTime.replace("Mar", "3");
    } else if (newTime.contains("Apr")) {
      newTime = newTime.replace("Apr", "4");
    } else if (newTime.contains("May")) {
      newTime = newTime.replace("May", "5");
    } else if (newTime.contains("Jun")) {
      newTime = newTime.replace("Jun", "6");
    } else if (newTime.contains("Jul")) {
      newTime = newTime.replace("Jul", "7");
    } else if (newTime.contains("Aug")) {
      newTime = newTime.replace("Aug", "8");
    } else if (newTime.contains("Sep")) {
      newTime = newTime.replace("Sep", "9");
    } else if (newTime.contains("Oct")) {
      newTime = newTime.replace("Oct", "10");
    } else if (newTime.contains("Nov")) {
      newTime = newTime.replace("Nov", "11");
    } else if (newTime.contains("Dec")) {
      newTime = newTime.replace("Dec", "12");
    }
    return newTime;
  }

  public void readFile() {

    String httplogpath = null;
    String ftplogpath = null;
    
    File directory = new File(props.getProperty(MudrodConstants.DATA_DIR));
    File[] fList = directory.listFiles();
    for (File file : fList) {
      if (file.isFile() && file.getName().contains(props.getProperty(MudrodConstants.TIME_SUFFIX))) 
      {
        if (file.getName().contains(props.getProperty(MudrodConstants.HTTP_PREFIX))) 
        {
          httplogpath = file.getAbsolutePath();
        }
        
        if (file.getName().contains(props.getProperty(MudrodConstants.FTP_PREFIX))) 
        {
          ftplogpath = file.getAbsolutePath();
        }
      }
    }
    
    if(httplogpath == null || ftplogpath == null)
    {
      LOG.error("WWW file or FTP logs cannot be found, please check your data directory.");
      return;
    }

    readFileInParallel(httplogpath, ftplogpath);
  }

  /**
   * Read the FTP or HTTP log path with the intention of processing lines from
   * log files.
   *
   * @param httplogpath path to the parent directory containing http logs
   * @param ftplogpath  path to the parent directory containing ftp logs
   */
  public void readFileInParallel(String httplogpath, String ftplogpath) {

    importHttpfile(httplogpath);
    importFtpfile(ftplogpath);
  }

  public void importHttpfile(String httplogpath) {
    // import http logs
    JavaRDD<String> accessLogs = spark.sc.textFile(httplogpath, this.partition).map(s -> ApacheAccessLog.parseFromLogLine(s, props)).filter(ApacheAccessLog::checknull);

    JavaEsSpark.saveJsonToEs(accessLogs, logIndex + "/" + this.httpType);
  }

  public void importFtpfile(String ftplogpath) {
    // import ftp logs
    JavaRDD<String> ftpLogs = spark.sc.textFile(ftplogpath, this.partition).map(s -> FtpLog.parseFromLogLine(s, props)).filter(FtpLog::checknull);

    JavaEsSpark.saveJsonToEs(ftpLogs, logIndex + "/" + this.ftpType);
  }

  @Override
  public Object execute(Object o) {
    return null;
  }
}

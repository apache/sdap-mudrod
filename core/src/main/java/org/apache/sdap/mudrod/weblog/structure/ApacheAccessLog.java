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

import com.google.gson.Gson;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.sdap.mudrod.weblog.pre.CrawlerDetection;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog extends WebLog implements Serializable {


  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public ApacheAccessLog() {
    //default constructor
  }

  String response;
  String referer;
  String browser;

  @Override
  public double getBytes() {
    return this.Bytes;
  }

  public String getBrowser() {
    return this.browser;
  }

  public String getResponse() {
    return this.response;
  }

  public String getReferer() {
    return this.referer;
  }


  public static String parseFromLogLine(String log) throws IOException, ParseException {

    String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";
    final int numFields = 9;
    Pattern p = Pattern.compile(logEntryPattern);
    Matcher matcher;

    String lineJson = "{}";
    matcher = p.matcher(log);
    if (!matcher.matches() || numFields != matcher.groupCount()) {
      return lineJson;
    }

    String time = matcher.group(4);
    time = SwithtoNum(time);
    SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
    Date date = formatter.parse(time);

    String bytes = matcher.group(7);

    if ("-".equals(bytes)) {
      bytes = "0";
    }

    String request = matcher.group(5).toLowerCase();
    String agent = matcher.group(9);
    CrawlerDetection crawlerDe = new CrawlerDetection();
    if (crawlerDe.checkKnownCrawler(agent)) {
      return lineJson;
    } else {

      String[] mimeTypes = { ".js", ".css", ".jpg", ".png", ".ico", "image_captcha", "autocomplete", ".gif", "/alldata/", "/api/", "get / http/1.1", ".jpeg", "/ws/" };
      for (String mimeType : mimeTypes) {
        if (request.contains(mimeType)) {
          return lineJson;
        }
      }

      ApacheAccessLog accesslog = new ApacheAccessLog();
      accesslog.LogType = "PO.DAAC";
      accesslog.IP = matcher.group(1);
      accesslog.Request = matcher.group(5);
      accesslog.response = matcher.group(6);
      accesslog.Bytes = Double.parseDouble(bytes);
      accesslog.referer = matcher.group(8);
      accesslog.browser = matcher.group(9);
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'");
      accesslog.Time = df.format(date);

      Gson gson = new Gson();
      lineJson = gson.toJson(accesslog);

      return lineJson;
    }
  }

  public static boolean checknull(WebLog s) {
    if (s == null) {
      return false;
    }
    return true;
  }

}

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
package org.apache.sdap.mudrod.weblog.structure.session;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.sdap.mudrod.main.MudrodConstants;

/**
 * Functions related to a node in a session tree sturcture.
 */
public class SessionNode {
  // id: Node ID
  protected String id;
  // value: Node value
  protected String value;
  // parent: Parent node of this node
  protected SessionNode parent;
  // children: Child nodes of this node
  protected List<SessionNode> children = new ArrayList<>();
  // time: request time of node
  protected String time;
  // request: request url of this node
  protected String request;
  // referer: previous request url of this node
  protected String referer;
  // seq: sequence of this node
  protected int seq;
  // key: type of this node extracted from url, including three types -
  // dataset,datasetlist,ftp
  protected String key;
  // logType: log types of this node, including two types - po.dacc, ftp
  protected String logType;
  // search: query extracted from this node
  protected String search;
  // filter: filter facets extracted from this node
  protected Map<String, String> filter;
  // datasetId: viewed/downloaded data set ID
  protected String datasetId;

  public SessionNode() {
    //default constuctor
  }

  /**
   * Creates a new instance of SessionNode.
   *
   * @param props a populated {@link java.util.Properties} object
   * @param request: request url
   * @param logType: including two types - po.dacc, ftp
   * @param referer: previous request url
   * @param time:    request time of node
   * @param seq:     sequence of this node
   */
  public SessionNode(Properties props, String request, String logType, String referer, String time, int seq) {
    this.logType = logType;
    this.time = time;
    this.seq = seq;
    this.setRequest(request);
    this.setReferer(referer, props.getProperty(MudrodConstants.BASE_URL));
    this.setKey(props, request, logType);
  }

  /**
   * Set previous request url of this node
   *
   * @param referer previous request url
   * @param basicUrl the referer URL string to replace with ""
   */
  public void setReferer(String referer, String basicUrl) {
    if (referer == null) {
      this.referer = "";
      return;
    }
    this.referer = referer.toLowerCase().replace(basicUrl, "");
  }

  /**
   * Set request url of this node
   *
   * @param req request url
   */
  public void setRequest(String req) {
    this.request = req;
    if (this.logType.equals(MudrodConstants.HTTP_LOG)) {
      this.parseRequest(req);
    }
  }

  /**
   * Get child nodes of this node
   *
   * @return child nodes
   */
  public List<SessionNode> getChildren() {
    return this.children;
  }

  /**
   * Set child nodes of this node
   *
   * @param children child nodes of this node
   */
  public void setChildren(List<SessionNode> children) {
    this.children = children;
  }

  /**
   * Add a children node
   *
   * @param node session node
   */
  public void addChildren(SessionNode node) {
    this.children.add(node);
  }

  /**
   * Get node ID
   *
   * @return node ID of this node
   */
  public String getId() {
    return this.id;
  }

  /**
   * Compare this node with another node
   *
   * @param node {@link SessionNode}
   * @return boolean value, true mean the two nodes are same
   */
  public Boolean bSame(SessionNode node) {
    Boolean bsame = false;
    if (this.request.equals(node.request)) {
      bsame = true;
    }
    return bsame;
  }

  /**
   * Set request type which contains three categories -
   * dataset,datasetlist,ftp
   *
   * @param props a populated {@link java.util.Properties} object
   * @param request request url
   * @param logType url type
   */
  public void setKey(Properties props, String request, String logType) {
    this.key = "";
    String datasetlist = props.getProperty(MudrodConstants.SEARCH_MARKER);
    String dataset = props.getProperty(MudrodConstants.VIEW_MARKER);
    if ("ftp".equals(logType)) {
      this.key = "ftp";
    } else if ("root".equals(logType)) {
      this.key = "root";
    } else {
      if (request.contains(datasetlist)) {
        this.key = MudrodConstants.SEARCH_MARKER;
      } else if (request.contains(dataset) /* || request.contains(granule) */) {
        this.key = MudrodConstants.VIEW_MARKER;
      }
    }
  }

  /**
   * Get request type which contains three categories -
   * dataset,datasetlist,ftp
   *
   * @return request url type of this node
   */
  public String getKey() {
    return this.key;
  }

  /**
   * Get node request
   *
   * @return request url of this node
   */
  public String getRequest() {
    return this.request;
  }

  /**
   * Get previous request url of this node
   *
   * @return previous request url of this node
   */
  public String getReferer() {
    return this.referer;
  }

  /**
   * Get parent node of this node
   *
   * @return parent node of this node
   */
  public SessionNode getParent() {
    return this.parent;
  }

  /**
   * Set parent node of this node
   *
   * @param parent the previous request node of this node
   */
  public void setParent(SessionNode parent) {
    this.parent = parent;
  }

  /**
   * Get query of this node
   *
   * @return search query of this node
   */
  public String getSearch() {
    return this.search;
  }

  /**
   * Get filter facets of this node
   *
   * @return filter values of this node
   */
  public Map<String, String> getFilter() {
    return this.filter;
  }

  /**
   * Get data set ID of this node
   *
   * @return viewing/downloading data set of this node
   */
  public String getDatasetId() {
    return this.datasetId;
  }

  /**
   * Get sequence of this node
   *
   * @return request sequence of this node
   */
  public int getSeq() {
    return this.seq;
  }

  /**
   * Get filter facets of this node
   *
   * @return filters values of this node
   */
  public String getFilterStr() {
    String filter = "";
    if (this.filter.size() > 0) {
      Iterator<String> iter = this.filter.keySet().iterator();
      while (iter.hasNext()) {
        String key = iter.next();
        String val = this.filter.get(key);
        filter += key + "=" + val + ",";
      }

      filter = filter.substring(0, filter.length() - 1);
    }

    return filter;
  }

  /**
   * Parse request to extract request type
   *
   * @param request request url of this node
   */
  public void parseRequest(String request) {
    Pattern pattern = Pattern.compile("get (.*?) http/*");
    Matcher matcher = pattern.matcher(request.trim().toLowerCase());
    while (matcher.find()) {
      request = matcher.group(1);
    }
    if (request.contains("/dataset/")) {
      this.parseDatasetId(request);
    }

    this.request = request.toLowerCase();
  }

  /**
   * Parse Request to extract data set ID
   *
   * @param request request url
   */
  public void parseDatasetId(String request) {
    try {
      request = URLDecoder.decode(request, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    String[] twoparts = request.split("[?]");
    String[] parts = twoparts[0].split("/");
    if (parts.length <= 2) {
      return;
    }
    this.datasetId = parts[2];
  }
}

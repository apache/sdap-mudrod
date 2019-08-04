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
package org.apache.sdap.mudrod.services;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.ssearch.Ranker;
import org.apache.sdap.mudrod.ssearch.Searcher;
import org.apache.sdap.mudrod.tools.EONETIngester;

import java.util.Properties;

/**
 * Application Lifecycle Listener implementation class MudrodContextListener
 */
@WebListener
public class MudrodContextListener implements ServletContextListener {

  private MudrodEngine me = null;

  /**
   * Default constructor.
   */
  public MudrodContextListener() {
    // default constructor
  }

  /**
   * @see ServletContextListener#contextDestroyed(ServletContextEvent)
   */
  @Override
  public void contextDestroyed(ServletContextEvent arg0) {
    me.end();
  }

  /**
   * @see ServletContextListener#contextInitialized(ServletContextEvent)
   */
  @Override
  public void contextInitialized(ServletContextEvent arg0) {
    me = new MudrodEngine();
    Properties props = me.loadConfig();
    me.setESDriver(new ESDriver(props));
    me.setSparkDriver(new SparkDriver(props));
    ESDriver es = me.getESDriver();

    ServletContext ctx = arg0.getServletContext();
    Searcher searcher = new Searcher(props, es, null);
    Ranker ranker = new Ranker(props, es, me.getSparkDriver());
    EONETIngester eonetIngester = new EONETIngester(props, es, null);
    ctx.setAttribute("MudrodInstance", me);
    ctx.setAttribute("MudrodSearcher", searcher);
    ctx.setAttribute("MudrodRanker", ranker);
    ctx.setAttribute("MudrodEONETIngester", eonetIngester);
  }

}

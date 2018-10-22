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
package org.apache.sdap.mudrod.services.eonet;

import java.io.InputStream;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.tools.EventIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * An endpoint to execute ingestion of <a href="https://eonet.sci.gsfc.nasa.gov/">
 * Earth Observatory Natural Event Tracker (EONET)</a> data into
 * the MUDROD search server.
 */
@Path("/event")
public class EONETIngestionResource {

  private static final Logger LOG = LoggerFactory.getLogger(EONETIngestionResource.class);
  private EventIngester eventIngester;
  private MudrodEngine mEngine;

  public EONETIngestionResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
    this.eventIngester = (EventIngester) sc.getAttribute("MudrodEventIngester");
  }

  @GET
  @Path("/status")
  @Produces(MediaType.TEXT_PLAIN)
  public Response status() {
    return Response.ok("<h1>This is MUDROD EONET Ingestion Resource: running correctly...</h1>").build();
  }

  @GET
  @Path("/ingestAllEonetEvents")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  public Response executeEONETIngestion() {
    String result = eventIngester.ingestAllEonetEvents(mEngine);
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

  /**
   * POST an {@link java.io.InputStream} JSON array of events.
   * It is important that the JSON complies with the following
   * structure
   * <pre>
   * {code 
   * [
   *  {
   *   "id": ""EONET_1014"",
   *   "title": ""Koryaksky Volcano, Russia"",
   *   "description": """",
   *   "link": ""https://eonet.sci.gsfc.nasa.gov/api/v2.1/events/EONET_1014"",
   *   "closed": ""2009-08-27T00:00:00Z"",
   *   "categories": [
   *     "{"id":12,"title":"Volcanoes"}",
   *     ...
   *   ],
   *   "sources": [
   *     "{"id":"SIVolcano","url":"http://volcano.si.edu/volcano.cfm?vn=300090"}",
   *     ...
   *   ],
   *   "geometries": [
   *     "{"date":"2008-12-23T00:00:00Z","type":"Point","coordinates":[158.712,53.321]}",
   *     ...
   *   ]
   *  }
   * ]
   * 
   * }
   * </pre>
   * @param is the {@link java.io.InputStream} containing the JSON snippet above.
   * @return a JSON string representing the response result.
   */
  @POST
  @Path("/ingestEventsJSON")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("*/*")
  public Response executeEventJSONIngestion(InputStream is) {
    String result = eventIngester.ingestEventsJSON(mEngine, is);
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

}

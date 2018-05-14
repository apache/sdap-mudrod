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

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.sdap.mudrod.main.MudrodEngine;
import org.apache.sdap.mudrod.tools.EONETIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * An endpoint to execute ingestion of <a href="https://eonet.sci.gsfc.nasa.gov/">
 * Earth Observatory Natural Event Tracker (EONET)</a> data into
 * the MUDROD search server.
 */
@Path("/eonet")
public class EONETIngestionResource {

  private static final Logger LOG = LoggerFactory.getLogger(EONETIngestionResource.class);
  private EONETIngester eonetIngester;
  private MudrodEngine mEngine;

  public EONETIngestionResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
    this.eonetIngester = (EONETIngester) sc.getAttribute("MudrodEONETIngester");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response.ok("<h1>This is MUDROD EONET Ingestion Resource: running correctly...</h1>").build();
  }

  @GET
  @Path("/ingestAllEvents")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response executeEONETIngestion() {
    String result = eonetIngester.acquireAllEvents(mEngine);
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

}

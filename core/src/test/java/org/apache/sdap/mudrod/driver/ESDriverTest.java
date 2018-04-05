package org.apache.sdap.mudrod.driver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.elasticsearch.client.Client;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESDriverTest {

	private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
	protected static final String ES_SETTINGS = "elastic_settings.json";
	protected static final String ES_MAPPINGS = "elastic_mappings.json";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testESDriver() {
		ESDriver es = new ESDriver();
	}

	@Test
	public void testESDriverProperties() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
	}

	@Test
	public void testCreateBulkProcessor() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		es.createBulkProcessor();
	}

	@Test
	public void testDestroyBulkProcessor() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		es.createBulkProcessor();
		es.destroyBulkProcessor();
	}

	@Test
	public void testPutMapping() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		
		InputStream settingsStream = getClass().getClassLoader().getResourceAsStream(ES_SETTINGS);
	    InputStream mappingsStream = getClass().getClassLoader().getResourceAsStream(ES_MAPPINGS);
	    
		JSONObject settingsJSON = null;
	    JSONObject mappingJSON = null;
	    
	    Properties props = mudrodEngine.loadConfig();
	    try {
	    		settingsJSON = new JSONObject(IOUtils.toString(settingsStream));
	    } catch (JSONException | IOException e1) {
	    		LOG.error("Error reading Elasticsearch settings!", e1);
	    }

	    try {
	    		mappingJSON = new JSONObject(IOUtils.toString(mappingsStream));
	    } catch (JSONException | IOException e1) {
	        LOG.error("Error reading Elasticsearch mappings!", e1);
	    }

	    try {
	        if (settingsJSON != null && mappingJSON != null) {
	          es.putMapping(props.getProperty(MudrodConstants.ES_INDEX_NAME), settingsJSON.toString(), mappingJSON.toString());
	        }
	    } catch (IOException e) {
	        LOG.error("Error entering Elasticsearch Mappings!", e);
	    }
	}

	@Test
	public void testCustomAnalyzingStringString() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		String str = "temp";
		
		try {
			es.customAnalyzing("mudrod", str);
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("Error!", e);
		}
	}
		

	@Test
	public void testCustomAnalyzingStringStringString() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		String str = "temp";
		
		try {
			es.customAnalyzing("mudrod", "cody", str);
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("Error!", e);
		}
	}

	@Test
	public void testCustomAnalyzingStringListOfString() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		
		List<String> customlist = new ArrayList<>();
		customlist.add("string_a");
		customlist.add("string_b");
			
		try {
			es.customAnalyzing("mudrod", customlist);
		} catch (InterruptedException | ExecutionException e) {
			LOG.error("Error!", e);
		}
	}

	@Test
	public void testDeleteAllByQuery() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		es.deleteAllByQuery("mudrod", "MetadataLinkage", QueryBuilders.matchAllQuery());
	}

	@Test
	public void testDeleteType() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig()); 
		es.deleteType("mudrod", "MetadataLinkage");
	}

	@Test
	public void testGetTypeListWithPrefix() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		es.getTypeListWithPrefix("podaacsession", "sessionstats");
	}

	@Test
	public void testGetIndexListWithPrefix() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		es.getIndexListWithPrefix("podaacsession");
	}

	@Test
	public void testSearchByQueryStringStringString() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		try {
			es.searchByQuery("mudrod", "MetadataLinkage", "temp");
		} catch(IOException | InterruptedException | ExecutionException e) {
			LOG.error("Error!", e);
		}
	}

	@Test
	public void testSearchByQueryStringStringStringBoolean() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		try {
			es.searchByQuery("mudrod", "MetadataLinkage", "temp",false);
		} catch(IOException | InterruptedException | ExecutionException e) {
			LOG.error("Error!", e);
		}
	}

	@Test
	public void testAutoComplete() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		es.autoComplete("mudrod", "term");
	}

	@Test
	public void testClose() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		es.close();
	}

	@Test
	public void testRefreshIndex() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		es.refreshIndex();
	}

	@Test
	public void testMakeClient() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		try {
			es.makeClient(mudrodEngine.loadConfig());
		} catch(IOException e) {
			LOG.error("Error!", e);
		}
	}

	@Test
	public void testMain() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetClient() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		es.getClient();
	}

	@Test
	public void testSetClient() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		try {
			Client client = es.makeClient(mudrodEngine.loadConfig());
			es.setClient(client);
		} catch(IOException e) {
			LOG.error("Error!",e);
		}
	}

	@Test
	public void testGetBulkProcessor() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		es.getBulkProcessor();
	}

	@Test
	public void testSetBulkProcessor() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		BulkProcessor begin = es.getBulkProcessor();
		es.setBulkProcessor(begin);
	}

	@Test
	public void testGenerateUpdateRequestStringStringStringStringObject() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		es.generateUpdateRequest("mudrod", "MetadataLinkage", "id_1", "temp", "string_a");
	}

	@Test
	public void testGenerateUpdateRequestStringStringStringMapOfStringObject() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		Map<String, Object> result = new HashMap<String, Object>();
		result.put("temp", "string_a");
		es.generateUpdateRequest("mudrod", "MetadataLinkage", "id_1", result);
	}

	@Test
	public void testGetDocCountStringStringArray() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());		
		
		String box[] = new String[] {"MetadataLinkage"};
		es.getDocCount("mudrod",box);
	}

	@Test
	public void testGetDocCountStringArrayStringArray() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		String begin[] = new String[] {"mudrod"};
		String box[] = new String[] {"MetadataLinkage"};
		es.getDocCount(begin,box);
	}

	@Test
	public void testGetDocCountStringArrayStringArrayQueryBuilder() {
		MudrodEngine mudrodEngine = new MudrodEngine();
		ESDriver es = new ESDriver(mudrodEngine.loadConfig());
		
		MatchAllQueryBuilder search = QueryBuilders.matchAllQuery();
		
		String begin[] = new String[] {"mudrod"};
		String box[] = new String[] {"MetadataLinkage"};
		es.getDocCount(begin,box,search);
	}

	@Test
	public void testObject() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetClass() {
		fail("Not yet implemented");
	}

	@Test
	public void testHashCode() {
		fail("Not yet implemented");
	}

	@Test
	public void testEquals() {
		fail("Not yet implemented");
	}

	@Test
	public void testClone() {
		fail("Not yet implemented");
	}

	@Test
	public void testToString() {
		fail("Not yet implemented");
	}

	@Test
	public void testNotify() {
		fail("Not yet implemented");
	}

	@Test
	public void testNotifyAll() {
		fail("Not yet implemented");
	}

	@Test
	public void testWaitLong() {
		fail("Not yet implemented");
	}

	@Test
	public void testWaitLongInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testWait() {
		fail("Not yet implemented");
	}

	@Test
	public void testFinalize() {
		fail("Not yet implemented");
	}

}

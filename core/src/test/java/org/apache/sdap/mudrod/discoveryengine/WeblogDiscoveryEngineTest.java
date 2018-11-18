package org.apache.sdap.mudrod.discoveryengine;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.sdap.mudrod.driver.ESDriver;
import org.apache.sdap.mudrod.driver.SparkDriver;
import org.apache.sdap.mudrod.main.AbstractElasticsearchIntegrationTest;
import org.apache.sdap.mudrod.main.MudrodConstants;
import org.apache.sdap.mudrod.main.MudrodEngine;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WeblogDiscoveryEngineTest extends AbstractElasticsearchIntegrationTest {
	
	
	private static WeblogDiscoveryEngine weblogEngine = null;
	
	@BeforeClass
    public static void setUp(){
		 MudrodEngine mudrodEngine = new MudrodEngine();
		 Properties props = mudrodEngine.loadConfig();
		 ESDriver es = new ESDriver(props);
		 SparkDriver spark = new SparkDriver(props);
		 File resourcesDirectory = new File("src/test/resources/");
		 String resourcedir = "/Testing_Data_1_3dayLog+Meta+Onto/";
		 String dataDir = resourcesDirectory.getAbsolutePath()+ resourcedir;
		 System.out.println(dataDir);
		 props.setProperty(MudrodConstants.DATA_DIR, dataDir);		 
		 MudrodEngine.loadPathConfig(mudrodEngine, dataDir);
		 weblogEngine = new WeblogDiscoveryEngine(props, es, spark);
    }
	
    @AfterClass
    public static void tearDown(){
        //TODO
    }
    
	@Test
	public void testPreprocess() throws IOException {
		weblogEngine.preprocess();
		//TODO compare session result
		//TODO compare history data
		String userHistorycsvFile = "C:/Users/admin/Documents/GitHub/incubator-sdap-mudrod/core/userHistoryMatrix.csv";
		System.out.println(userHistorycsvFile);
		BufferedReader br = new BufferedReader(new FileReader(userHistorycsvFile));
	    String line =  null;
	    HashMap<String,List<String>> map = new HashMap<>();

	    int i =0;
	    List<String> header = new LinkedList<>();
	    while((line=br.readLine())!=null){	    	
	    	if(i ==0){
	    		String str[] = line.split(",");
	    		for(String s : str){
	    			header.add(s);
	    		}
	    	}else{	    	
	        String str[] = line.split(",");
	        for(int j=1;j<str.length;j++){
//	        	System.out.println(str[j]+str[j].getClass().getName());
	        	if (!str[j].equals("0")){
//	        		System.out.println(str[0]+" "+header.get(j));
	        		if(!map.containsKey(str[0])){
	        			map.put(str[0], new ArrayList<>());
	        		}
	        		map.get(str[0]).add(header.get(j));
	        	}
	        }
	    	}	        
	        i += 1;
	    }
	    System.out.println(map);
	    
//	    Assert.assertEquals("failed in history data result!", "202.92.130.58",String.join(",", map.get("metopglobal")));
//	    Assert.assertEquals("failed in history data result!", "202.92.130.58",String.join(",", map.get("metop")));
    
	}
	
	@Test
	public void testPreprocess_clickStream() throws IOException {
		//TODO compare clickStream data
		String clickStreamcsvFile = "C:/Users/admin/Documents/GitHub/incubator-sdap-mudrod/core/clickStreamMatrix.csv";
		System.out.println(clickStreamcsvFile);
		BufferedReader br = new BufferedReader(new FileReader(clickStreamcsvFile));
	    String line =  null;
	    HashMap<String,List<String>> map = new HashMap<>();

	    int i =0;
	    List<String> header = new LinkedList<>();
	    while((line=br.readLine())!=null){	    	
	    	if(i == 0){
	    		String str[] = line.split(",");
	    		for(String s : str){
	    			header.add(s);
//	    			System.out.println(header);
	    		}
	    	}else{	    	
	        String str[] = line.split(",");
	        for(int j=1;j<str.length;j++){
//	        	System.out.println(str[j]+str[j].getClass().getName());
	        	
	        	if (!str[j].equals("0.0")){ 	//
	        		if(!map.containsKey(str[0])){
	        			map.put(str[0], new ArrayList<>());
	        		}
	        		map.get(str[0]).add(header.get(j));
	        	}
	        }
	    	}	        
	        i += 1;
	    }
	    System.out.println(map);
	    
	    Assert.assertEquals("failed in click stream result!", "\"avhrr_sst_noaa19_nar-osisaf-l3c-v1.0\"",String.join(",", map.get("avhrr sst noaa19 nar osisaf l3c v1.0")));
	    Assert.assertEquals("failed in click stream result!", "\"ascata-l2-coastal\"", String.join(",", map.get("metop")));
	}

//	@Test
//	public void testProcess() {
//		
//		//fail("Not yet implemented");
//	}
//
//	@Test
//	public void testWeblogDiscoveryEngine() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testGetFileList() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testLogIngest() {
//		fail("Not yet implemented");
//	}
//
//	@Test
//	public void testSessionRestruct() {
//		fail("Not yet implemented");
//	}

}

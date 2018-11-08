package org.apache.sdap.mudrod.weblog.structure.log;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.sdap.mudrod.weblog.structure.log.RequestUrl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RequestUrlTest {
	
//	@BeforeClass
//    public void setUp(){
//		RequestUrl url = new RequestUrl();
//		
//    }
//	@Test
//	public void testRequestUrl() {
//		fail("Not yet implemented");
//	}

	@Test
	public void testUrlPage() {
		RequestUrl url = new RequestUrl();
		String strURL = "https://podaac.jpl.nasa.gov/datasetlist?ids=Collections:Measurement:SpatialCoverage:Platform:Sensor&values=SCAT_BYU_L3_OW_SIGMA0_ENHANCED:Sea%20Ice:Bering%20Sea:ERS-2:AMI&view=list";
		String result = url.urlPage(strURL);
		//System.out.println(urlPage);
		///fail("Not yet implemented");
		Assert.assertEquals("You did not pass urlPage function ", "https://podaac.jpl.nasa.gov/datasetlist", result);
	}

	@Test
	public void testuRLRequest() {
		RequestUrl url = new RequestUrl();
		String strURL = "https://podaac.jpl.nasa.gov/datasetlist?ids=Collections:Measurement:SpatialCoverage:Platform:Sensor&values=SCAT_BYU_L3_OW_SIGMA0_ENHANCED:Sea%20Ice:Bering%20Sea:ERS-2:AMI&view=list";
		Map<String, String> result = url.uRLRequest(strURL);
		Assert.assertEquals("You did not pass uRLRequest function!","list", result.get("view"));
		Assert.assertEquals("You did not pass uRLRequest function!","scat_byu_l3_ow_sigma0_enhanced:sea%20ice:bering%20sea:ers-2:ami", result.get("values"));
		Assert.assertEquals("You did not pass uRLRequest function!","collections:measurement:spatialcoverage:platform:sensor", result.get("ids"));
	}

	@Test
	public void testGetSearchInfo() {
		RequestUrl url = new RequestUrl();
		String strURL = "https://podaac.jpl.nasa.gov/datasetlist?ids=Collections:Measurement:SpatialCoverage:Platform:Sensor&values=SCAT_BYU_L3_OW_SIGMA0_ENHANCED:Sea%20Ice:Bering%20Sea:ERS-2:AMI&view=list";
		String result = null;
		try {
			result = url.getSearchInfo(strURL);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Assert.assertEquals("You did not pass GetSearchInfo function ", "scat byu l3 ow sigma0 enhanced,sea ice,bering sea,ers 2,ami", result);
	}

	@Test
	public void testGetFilterInfo() {
		RequestUrl url = new RequestUrl();
		String strURL = "https://podaac.jpl.nasa.gov/datasetlist?ids=Collections:Measurement:SpatialCoverage:Platform:Sensor&values=SCAT_BYU_L3_OW_SIGMA0_ENHANCED:Sea%20Ice:Bering%20Sea:ERS-2:AMI&view=list";
		try {
			Map<String, String> result = url.getFilterInfo(strURL);
			Assert.assertEquals("You did not pass GetFilterInfo function!","scat byu l3 ow sigma0 enhanced", result.get("collections"));
			Assert.assertEquals("You did not pass GetFilterInfo function!","bering sea", result.get("spatialcoverage"));
			Assert.assertEquals("You did not pass GetFilterInfo function!","ami", result.get("sensor"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

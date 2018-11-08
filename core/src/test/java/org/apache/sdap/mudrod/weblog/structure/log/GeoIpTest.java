package org.apache.sdap.mudrod.weblog.structure.log;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

public class GeoIpTest {

	@Test
	public void testToLocation() {
		GeoIp ip = new GeoIp();
		String iptest = "185.10.104.194";
		Coordinates result = ip.toLocation(iptest);
		Assert.assertEquals("failed in geoip function!", "22.283001,114.150002", result.latlon);
	}

}

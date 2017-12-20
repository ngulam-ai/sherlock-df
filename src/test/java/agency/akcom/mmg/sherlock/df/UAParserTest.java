package agency.akcom.mmg.sherlock.df;

import java.io.IOException;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;





@RunWith(JUnit4.class)
public class UAParserTest {

	private static final String USER_AGENT_STRING_WITH_DEVICE_NAME = "Mozilla/5.0 (Linux; Android 7.0; SAMSUNG SM-G950F Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/5.4 Chrome/51.0.2704.106 Mobile Safari/537.36";
	private static final String JSON_WITH_DEVICE_NAME = "{\"__istc\":true,\"__db\":\"Samsung\",\"__dc\":\"Mobile Phone\",\"__dm\":\"Galaxy S8\",\"__bf\":\"Samsung Browser\",\"__bv\":\"5.4\",\"__of\":\"Android\",\"__ov\":\"7.0\",\"__isb\":false}";
	
	private static final String USER_AGENT_STRING_1 = "Mozilla/5.0 (Linux; Android 5.1; LENNY2 Build/LMY47I; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/62.0.3202.84 Mobile Safari/537.36";
	private static final String JSON_1 = "{\"__istc\":true,\"__db\":\"Unknown\",\"__dc\":\"Mobile Phone\",\"__dm\":\"general Mobile Phone\",\"__bf\":\"Android WebView\",\"__bv\":\"4.0\",\"__of\":\"Android\",\"__ov\":\"5.1\",\"__isb\":false}";
	
	private static final String USER_AGENT_STRING_2 = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.111 Safari/537.36";
	private static final String JSON_2 = "{\"__istc\":false,\"__db\":\"Unknown\",\"__dc\":\"Desktop\",\"__dm\":\"Linux Desktop\",\"__bf\":\"Chrome\",\"__bv\":\"63.0\",\"__of\":\"Linux\",\"__ov\":\"Unknown\",\"__isb\":false}";
	
	private static final String USER_AGENT_STRING_3 = "Mozilla/5.0 (Linux; Android 6.0.1; Aquaris U Lite Build/MMB29M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.111 Mobile Safari/537.36";
	private static final String JSON_3 = "{\"__istc\":true,\"__db\":\"Unknown\",\"__dc\":\"Mobile Phone\",\"__dm\":\"general Mobile Phone\",\"__bf\":\"Chrome\",\"__bv\":\"63.0\",\"__of\":\"Android\",\"__ov\":\"6.0\",\"__isb\":false}";
	
	@Test
	public void testParseUserAgent() throws IOException {		
		JSONObject elementJSON = new JSONObject();
		UAParser.parseUserAgent(elementJSON, USER_AGENT_STRING_WITH_DEVICE_NAME);
		Assert.assertEquals(JSON_WITH_DEVICE_NAME, elementJSON.toString());
		
		elementJSON = new JSONObject();
		UAParser.parseUserAgent(elementJSON, USER_AGENT_STRING_1);
		Assert.assertEquals(JSON_1, elementJSON.toString());
		
		elementJSON = new JSONObject();
		UAParser.parseUserAgent(elementJSON, USER_AGENT_STRING_2);
		Assert.assertEquals(JSON_2, elementJSON.toString());
		
		elementJSON = new JSONObject();
		UAParser.parseUserAgent(elementJSON, USER_AGENT_STRING_3);
		Assert.assertEquals(JSON_3, elementJSON.toString());
	}
}

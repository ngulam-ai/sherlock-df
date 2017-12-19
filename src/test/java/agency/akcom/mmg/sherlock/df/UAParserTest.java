package agency.akcom.mmg.sherlock.df;

import java.io.IOException;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UAParserTest {

	private static final String USER_AGENT_STRING = "Mozilla/5.0 (Linux; Android 7.0; SAMSUNG SM-G950F Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/5.4 Chrome/51.0.2704.106 Mobile Safari/537.36";
	@Test
	public void testParseUserAgent() throws IOException {		
		JSONObject elementJSON = new JSONObject();
		UAParser.parseUserAgent(elementJSON, USER_AGENT_STRING);
	}
}

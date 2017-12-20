package agency.akcom.mmg.sherlock.df;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.blueconic.browscap.BrowsCapField;
import com.blueconic.browscap.Capabilities;
import com.blueconic.browscap.ParseException;
import com.blueconic.browscap.UserAgentParser;
import com.blueconic.browscap.UserAgentService;

public class UAParser {
	
	private static final Logger LOG = LoggerFactory.getLogger(TableRowCreator.class);
	
	private static final UserAgentParser bcParser = getParser();	

	public static void parseUserAgent(JSONObject elementJSON, String userAgent)  {

		final Capabilities capabilities = getParser().parse(userAgent);

		// log all values for debugging
//		String parsedValues = "";
//		for (Entry<BrowsCapField, String> entry : capabilities.getValues().entrySet()) {
//			parsedValues += entry.getKey() + ": " + entry.getValue() + "\r\n";
//		}
//		LOG.info(parsedValues);
			
		elementJSON.put("__bf", capabilities.getBrowser()); // browserFamily
		elementJSON.put("__bv", capabilities.getValue(BrowsCapField.BROWSER_VERSION)); // browserVersion
		
		elementJSON.put("__of", capabilities.getPlatform()); // osFamily
		elementJSON.put("__ov", capabilities.getPlatformVersion()); // osVersion
		
		elementJSON.put("__db", capabilities.getValue(BrowsCapField.DEVICE_BRAND_NAME)); // deviceBrand
		elementJSON.put("__dm", capabilities.getValue(BrowsCapField.DEVICE_NAME)); // deviceModel
		elementJSON.put("__dc", capabilities.getDeviceType()); // deviceCategory
		
		elementJSON.put("__isb", "true".equals(capabilities.getValue(BrowsCapField.IS_CRAWLER))); // isBot
		//elementJSON.put("__isec",agent.getType().equals(UserAgentType.EMAIL_CLIENT)); // isEmailClient		
		elementJSON.put("__istc", "touchscreen".equals(capabilities.getValue(BrowsCapField.DEVICE_POINTING_METHOD))); // isTouchCapable

//				{"device.ip","STRING", "NULLABLE","uip, __uip",""}	,
//				{"device.flashVersion","STRING", "NULLABLE","fl, __fl","ga:flashVersion"}	,
//				{"device.javaEnabled","INTEGER", "NULLABLE", "__je","ga:javaEnabled"}	, // 'je' - TODO need to implement converting to boolean
//				{"device.language","STRING", "NULLABLE","ul, __ul","ga:language"}	,
//				{"device.screenColors","STRING", "NULLABLE","sd, __sd","ga:screenColors"}	,
//				{"device.screenResolution","STRING", "NULLABLE","sr, __sr","ga:screenResolution"}	,
//				{"device.viewPort","STRING", "NULLABLE","__vp",""}	,
//				{"device.encoding","STRING", "NULLABLE","__enc",""}	,
		
		LOG.info("UAParser.parseUserAgent(): " + elementJSON.toString());
	}


	private static UserAgentParser getParser() {
		if (bcParser == null) {
			try {		
				// create a parser with a custom defined field list
				// the list of available fields can be seen in the BrowsCapField enum
				return new UserAgentService().loadParser(Arrays.asList(BrowsCapField.IS_MASTER_PARENT,
						BrowsCapField.IS_LITE_MODE,
						BrowsCapField.PARENT,
						BrowsCapField.COMMENT,
						BrowsCapField.BROWSER,
						BrowsCapField.BROWSER_TYPE,
						BrowsCapField.BROWSER_BITS,
						BrowsCapField.BROWSER_MAKER,
						BrowsCapField.BROWSER_MODUS,
						BrowsCapField.BROWSER_VERSION,
						BrowsCapField.BROWSER_MAJOR_VERSION,
						BrowsCapField.BROWSER_MINOR_VERSION,
						BrowsCapField.PLATFORM,
						BrowsCapField.PLATFORM_VERSION,
						BrowsCapField.PLATFORM_DESCRIPTION,
						BrowsCapField.PLATFORM_BITS,
						BrowsCapField.PLATFORM_MAKER,
						BrowsCapField.IS_ALPHA,
						BrowsCapField.IS_BETA,
						BrowsCapField.IS_WIN16,
						BrowsCapField.IS_WIN32,
						BrowsCapField.IS_WIN64,
						BrowsCapField.IS_IFRAMES,
						BrowsCapField.IS_FRAMES,
						BrowsCapField.IS_TABLES,
						BrowsCapField.IS_COOKIES,
						BrowsCapField.IS_BACKGROUND_SOUNDS,
						BrowsCapField.IS_JAVASCRIPT,
						BrowsCapField.IS_VBSCRIPT,
						BrowsCapField.IS_JAVA_APPLETS,
						BrowsCapField.IS_ACTIVEX_CONTROLS,
						BrowsCapField.IS_MOBILE_DEVICE,
						BrowsCapField.IS_TABLET,
						BrowsCapField.IS_SYNDICATION_READER,
						BrowsCapField.IS_CRAWLER,
						BrowsCapField.IS_FAKE,
						BrowsCapField.IS_ANONYMIZED,
						BrowsCapField.IS_MODIFIED,
						BrowsCapField.CSS_VERSION,
						BrowsCapField.AOL_VERSION,
						BrowsCapField.DEVICE_NAME,
						BrowsCapField.DEVICE_MAKER,
						BrowsCapField.DEVICE_TYPE,
						BrowsCapField.DEVICE_POINTING_METHOD,
						BrowsCapField.DEVICE_CODE_NAME,
						BrowsCapField.DEVICE_BRAND_NAME,
						BrowsCapField.RENDERING_ENGINE_NAME,
						BrowsCapField.RENDERING_ENGINE_VERSION,
						BrowsCapField.RENDERING_ENGINE_DESCRIPTION));
			} catch (IOException | ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return bcParser;
	}
}



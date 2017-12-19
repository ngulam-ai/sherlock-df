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
import com.blueconic.browscap.UserAgentService;

public class UAParser {
	private JSONObject elementJSON;
	
	private static final Logger LOG = LoggerFactory.getLogger(TableRowCreator.class);

	public static void parseUserAgent(JSONObject elementJSON, String userAgent)  {
		
		// create a parser with a custom defined field list
		// the list of available fields can be seen inthe BrowsCapField enum
		com.blueconic.browscap.UserAgentParser bcParser;
		try {
			bcParser = new UserAgentService().loadParser(Arrays.asList(BrowsCapField.IS_MASTER_PARENT,
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
			
			final Capabilities capabilities = bcParser.parse(userAgent);
			
			for (Entry<BrowsCapField, String> entry : capabilities.getValues().entrySet()) {
				LOG.info(entry.getKey() + " " + entry.getValue());
			}
			
		} catch (IOException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
//		elementJSON.put("__bf", agent.getName()); // browserFamily
//		elementJSON.put("__bv", agent.getVersionNumber().toVersionString()); // browserVersion
//		
//		elementJSON.put("__of", agent.getOperatingSystem().getName()); // osFamily
//		elementJSON.put("__ov", agent.getOperatingSystem().getVersionNumber().toVersionString()); // osVersion
		
		//elementJSON.put("__db", agent.getProducer()); // deviceBrand
		//elementJSON.put("__dm", agent.getFamily().getName()); // deviceModel
//		elementJSON.put("__dc", agent.getDeviceCategory().getName()); // deviceCategory
//		
//		elementJSON.put("__isb", agent.getType().equals(UserAgentType.ROBOT)); // isBot
//		elementJSON.put("__isec",agent.getType().equals(UserAgentType.EMAIL_CLIENT)); // isEmailClient
//		
//				{"device.isBot","STRING", "NULLABLE","__isb",""}	,
//				{"device.isEmailClient","STRING", "NULLABLE","__isec",""}	,

		
//			       System.out.println("- - - - - - - - - - - - - - - - -");
//			        // type
//			        System.out.println("Browser type: " + agent.getType().getName());
//			        System.out.println("Browser name: " + agent.getName());
//			        VersionNumber browserVersion = agent.getVersionNumber();
//			        System.out.println("Browser version: " + browserVersion.toVersionString());
//			        System.out.println("Browser version major: " + browserVersion.getMajor());
//			        System.out.println("Browser version minor: " + browserVersion.getMinor());
//			        System.out.println("Browser version bug fix: " + browserVersion.getBugfix());
//			        System.out.println("Browser version extension: " + browserVersion.getExtension());
//			        System.out.println("Browser producer: " + agent.getProducer());
//
//			        // operating system
//			        OperatingSystem os = agent.getOperatingSystem();
//			        System.out.println("\nOS Name: " + os.getName());
//			        System.out.println("OS Producer: " + os.getProducer());
//			        VersionNumber osVersion = os.getVersionNumber();
//			        System.out.println("OS version: " + osVersion.toVersionString());
//			        System.out.println("OS version major: " + osVersion.getMajor());
//			        System.out.println("OS version minor: " + osVersion.getMinor());
//			        System.out.println("OS version bug fix: " + osVersion.getBugfix());
//			        System.out.println("OS version extension: " + osVersion.getExtension());
//
//			        // device category
//			        ReadableDeviceCategory device = agent.getDeviceCategory();
//			        System.out.println("\nDevice: " + device.getName());
		
//				{"device.ip","STRING", "NULLABLE","uip, __uip",""}	,


//				{"device.deviceBrand","STRING", "NULLABLE","__db",""}	,
//				{"device.deviceModel","STRING", "NULLABLE","__dm",""}	,
//				{"device.deviceCategory","STRING", "NULLABLE","__dc",""}	,

//				{"device.isTouchCapable","STRING", "NULLABLE","__istc",""}	,
//				{"device.isBot","STRING", "NULLABLE","__isb",""}	,
//				{"device.isEmailClient","STRING", "NULLABLE","__isec",""}	,
//				{"device.flashVersion","STRING", "NULLABLE","fl, __fl","ga:flashVersion"}	,
//				{"device.javaEnabled","INTEGER", "NULLABLE", "__je","ga:javaEnabled"}	, // 'je' - TODO need to implement converting to boolean
//				{"device.language","STRING", "NULLABLE","ul, __ul","ga:language"}	,
//				{"device.screenColors","STRING", "NULLABLE","sd, __sd","ga:screenColors"}	,
//				{"device.screenResolution","STRING", "NULLABLE","sr, __sr","ga:screenResolution"}	,
//				{"device.viewPort","STRING", "NULLABLE","__vp",""}	,
//				{"device.encoding","STRING", "NULLABLE","__enc",""}	,
	}
}



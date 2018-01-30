package agency.akcom.mmg.sherlock.df;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

public class SpeedPerformanceTest {
	
	static JSONObject jsonObject = new JSONObject();

	static {
		JSONObject geoJSON = new JSONObject();	
		geoJSON.put("id", "");
		geoJSON.put("name", "testName");
		jsonObject.put("geo", geoJSON);
		JSONObject customDimensionsJSON = new JSONObject();
		customDimensionsJSON.put("1", "Lao People's Democratic Republic;Louangphabang;Louangphrabang;unknown;unknown;Android;5.1.1;Mozilla/5.0 (Linux; Android 5.1.1; SM-J200GU Build/LMY47X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/64.0.3282.123 Mobile Safari/537.36;115.84.115.224;$$CUSTOM_PARAM(cd11)$$;$$CUSTOM_PARAM(cd7)$$;$$CUSTOM_PARAM(cd10)$$\\");
		customDimensionsJSON.put("2", "Unknown");
		customDimensionsJSON.put("3", "4162917 - New Top Mainstream Dynamic Global");
		customDimensionsJSON.put("4", "n/a");
		customDimensionsJSON.put("5", "{fdsdf3#$}");
		customDimensionsJSON.put("6", "@1517199332861.c6me18qw@");
		customDimensionsJSON.put("70", "unknown");
		customDimensionsJSON.put("82", "Mozilla/5.0 (Linux; Android 5.1.1; SM-J200GU Build/LMY47X; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/64.0.3282.123 Mobile Safari/537.36");
		customDimensionsJSON.put("94", "$$,khgj5y3h$$");
		customDimensionsJSON.put("95", "${3252 t34}");
		customDimensionsJSON.put("95", "754/234.453");
		jsonObject.put("customDimensions", customDimensionsJSON);
		JSONObject customGroupsJSON = new JSONObject();
		customGroupsJSON.put("null", "null");
		jsonObject.put("customGroups", customGroupsJSON);
	}

	static String patternStr = "(n/a)|(unknown)|(Unknown)"
			+ "|(^\\${2}[^\\$].+[^\\$]\\${2}$)"  //$$xxx$$
			+ "|(^\\$.+$)" 						//${xxx}
			+ "|(^@.+@$)"						//@xxx@
			+ "|(^\\{.+\\}$)";					//{xxx}
	
	static Pattern pattern = Pattern.compile(patternStr);
	
	
	public static void processElement(JSONObject elementJSON) throws Exception {
		
		List<String> keysForReplace = new ArrayList<>();
		
		for (String key : elementJSON.getJSONObject("customDimensions").keySet()) {
			
			String value = elementJSON.getJSONObject("customDimensions").getString(key);
			
			if(value.contains("n/a") || value.contains("unknown") || value.contains("Unknown")) {
				keysForReplace.add(key);
			} else if ( (value.startsWith("${") || value.startsWith("{") ) && value.endsWith("}")) {
				keysForReplace.add(key);
			} else if ( value.startsWith("$$") && value.endsWith("$$")) {
				keysForReplace.add(key);
			} else if ( value.startsWith("@") && value.endsWith("@")) {
				keysForReplace.add(key);
			} 
		}
	}
	
	//NOTE: if it is faster, add method sub for replace string
	public static void processRegularExpression (JSONObject elementJSON) {
		    
		List<String> keysForReplace = new ArrayList<>();
		
		for (String key : elementJSON.getJSONObject("customDimensions").keySet()) {
			
			String value = elementJSON.getJSONObject("customDimensions").getString(key);
			
			Matcher matcher = pattern.matcher(value);
						
			if(matcher.find()) {
				for (int i = 1; i < 8; i++) {
					if(matcher.group(i)!=null) {
						keysForReplace.add(matcher.group(i));
					}
				}
			} 
		}
	}
   	
	public static void main(String[] args) throws Exception {

		long startValueContains = System.currentTimeMillis();
		
		for (int i = 0; i < 1500000; i++) {
			processElement(jsonObject);
		}
		System.out.println("time working processElement = " + (System.currentTimeMillis() - startValueContains));
		
		long startRegular = System.currentTimeMillis();
		for (int i = 0; i < 1500000; i++) {
			processRegularExpression(jsonObject);
		}
		System.out.println("time working processRegularExpression = " + (System.currentTimeMillis() - startRegular));
	}
	// RESULT: for count 150000 processElement = 309ms, processRegularExpression = 4643ms
//			   for count 1500000 processElement = 1726ms , processRegularExpression = 30254ms
}
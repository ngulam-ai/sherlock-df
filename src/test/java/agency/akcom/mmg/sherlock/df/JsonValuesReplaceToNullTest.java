package agency.akcom.mmg.sherlock.df;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import agency.akcom.mmg.sherlock.df.SherlockPipeline.JsonValuesReplaceToNull;

@RunWith(JUnit4.class)
public class JsonValuesReplaceToNullTest {
	
	static final JSONObject JSONINPUT = new JSONObject();
	static final JSONObject JSONOUTPUT = new JSONObject();
	
	static {
		JSONObject geoJSON = new JSONObject();	
		geoJSON.put("id", "");
		geoJSON.put("name", "testName");
		JSONINPUT.put("geo", geoJSON);
		JSONOUTPUT.put("geo", geoJSON);
		JSONObject customDimensionsJSONINPUT = new JSONObject();
		customDimensionsJSONINPUT.put("1", "n/a");
		customDimensionsJSONINPUT.put("2", "Unknown");
		customDimensionsJSONINPUT.put("3", "unknown");
		customDimensionsJSONINPUT.put("4", "$$test_test$$");
		customDimensionsJSONINPUT.put("5", "${test_test}");
		customDimensionsJSONINPUT.put("6", "@test_test@");
		customDimensionsJSONINPUT.put("7", "{test_test}");
		customDimensionsJSONINPUT.put("70", "Japan;Fukuoka;Kitakyushu;unknown;unknown;Android;6.0;Mozilla/5.0 (Linux; Android 6.0; FlareA1 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/51.0.2704.81 Mobile Safari/537.36;111.239.125.249;$$CUSTOM_PARAM(cd11)$$;$$CUSTOM_PARAM(cd7)$$;$$CUSTOM_PARAM(cd10)$$");
		customDimensionsJSONINPUT.put("82", "http://vfcyd.tjnet.es/cig/campaign/prelandingmm.php?actionID=MTYjMiMzIzQwNnwxMDR8RVN8M3w1fHxZV04wYVc5dVgwbEUqTkRNd09ERTJNak0zTXpneXxmOHExODU5YWEwcDR8ZGFjNWU3YTAtZmRmNC0xMWU3LTg2NjYtZDQ4NTY0YzYyZjQ0fHxjb20uZ2VuZXJhbW9iaWxlLmxvZ29tYW5pYQ&clickid=$$CUSTOM_PARAM(click_ID)$$&PublisherName=Propellerads&PlacementName=Prelanding&utm_source=Propellerads&utm_campagin=Prelanding");
		customDimensionsJSONINPUT.put("94", "unknown;n/a;$$test_test$$;${test_test};{test_test};Unknown;${test_test};{test_test};$$test_test$$;${test_test}");
		JSONINPUT.put("customDimensions", customDimensionsJSONINPUT);
		JSONObject customGroupsJSON = new JSONObject();
		JSONINPUT.put("customGroups", customGroupsJSON);
		JSONObject customDimensionsJSONOUTPUT = new JSONObject();
		customDimensionsJSONOUTPUT.put("1", JSONObject.NULL);
		customDimensionsJSONOUTPUT.put("2", JSONObject.NULL);
		customDimensionsJSONOUTPUT.put("3", JSONObject.NULL);
		customDimensionsJSONOUTPUT.put("4", JSONObject.NULL);
		customDimensionsJSONOUTPUT.put("5", JSONObject.NULL);
		customDimensionsJSONOUTPUT.put("6", JSONObject.NULL);
		customDimensionsJSONOUTPUT.put("7", JSONObject.NULL);
		customDimensionsJSONOUTPUT.put("70", "Japan;Fukuoka;Kitakyushu;null;null;Android;6.0;Mozilla/5.0 (Linux; Android 6.0; FlareA1 Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/51.0.2704.81 Mobile Safari/537.36;111.239.125.249;null;null;null");
		customDimensionsJSONOUTPUT.put("82", "http://vfcyd.tjnet.es/cig/campaign/prelandingmm.php?actionID=MTYjMiMzIzQwNnwxMDR8RVN8M3w1fHxZV04wYVc5dVgwbEUqTkRNd09ERTJNak0zTXpneXxmOHExODU5YWEwcDR8ZGFjNWU3YTAtZmRmNC0xMWU3LTg2NjYtZDQ4NTY0YzYyZjQ0fHxjb20uZ2VuZXJhbW9iaWxlLmxvZ29tYW5pYQ&clickid=null&PublisherName=Propellerads&PlacementName=Prelanding&utm_source=Propellerads&utm_campagin=Prelanding");
		customDimensionsJSONOUTPUT.put("94", "null;null;null;null;null;null;null;null;null;null");
		JSONOUTPUT.put("customDimensions", customDimensionsJSONOUTPUT);
		JSONOUTPUT.put("customGroups", customGroupsJSON);
	}	
			
	@Test
	public void testJsonReplaceToNull() {
		
		Pipeline pipeline = TestPipeline.create();
		
		PCollection<String> input = pipeline.apply(Create.of(JSONINPUT.toString())).setCoder(StringUtf8Coder.of());
		
		PCollection<String> output = input.apply(ParDo.of(new JsonValuesReplaceToNull()));
		
		DataflowAssert.that(output).containsInAnyOrder(JSONOUTPUT.toString());
		
		pipeline.run();
		
	}
}


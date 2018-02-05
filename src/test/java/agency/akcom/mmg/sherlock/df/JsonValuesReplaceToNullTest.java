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
	
	static final String STRIN = "{\"date\":\"20180205\",\"__dm\":\"Unknown\",\"__bf\":\"Asynchronous HTTP Client\",\"pr1qt\":\"1\",\"ua\":\"AHC/2.0\",\"tid\":\"UA-98835143-2\",\"uid\":\";;;unknown;unknown;unknown;unknown;;;$$CUSTOM_PARAM(cd11)$$;{sha1_did};{ifa}\",\"pr1ca\":\"Gaming\",\"pr1id\":\"329\",\"hour\":\"02\",\"__bv\":\"0.0\",\"__uip\":\"212.124.119.131\",\"__of\":\"Unknown\",\"ea\":\"Conversion\",\"ec\":\"Conversion\",\"pr1br\":\"Mobidea\",\"hitId\":\"71ef2646-ce0e-46b1-b168-b4093c560da7\",\"cm\":\"Pop\",\"cn\":\"Avazu MDSP|IT | Rome Milan Unknown | TIM WIND Vodafone | Phone:Alcatel Samsung Generic | Android:6.0 7.0 | Chrome Mobile\",\"__ov\":\"Unknown\",\"ta\":\"Mobidea\",\"pr1pr\":\"1.75\",\"minute\":\"27\",\"pr1nm\":\"17228 - Games - IT - 3G - Playoo Angry Fish\",\"cs\":\"Avazu MDSP\",\"pa\":\"purchase\",\"__istc\":false,\"__db\":\"Unknown\",\"__dc\":\"Unknown\",\"t\":\"event\",\"ti\":\"pumies75dfj7\",\"v\":\"1\",\"time\":\"1517794059508\",\"tr\":\"1.75\",\"__isb\":true}";
	static final JSONObject JSONINPUT = new JSONObject(STRIN);
	static final String STROUT = "{\"date\":\"20180205\",\"__dm\":null,\"__bf\":\"Asynchronous HTTP Client\",\"pr1qt\":\"1\",\"ua\":\"AHC/2.0\",\"tid\":\"UA-98835143-2\",\"uid\":\";;;null;null;null;null;;;null;null;null\",\"pr1ca\":\"Gaming\",\"pr1id\":\"329\",\"hour\":\"02\",\"__bv\":\"0.0\",\"__uip\":\"212.124.119.131\",\"__of\":null,\"ea\":\"Conversion\",\"ec\":\"Conversion\",\"pr1br\":\"Mobidea\",\"hitId\":\"71ef2646-ce0e-46b1-b168-b4093c560da7\",\"cm\":\"Pop\",\"cn\":\"Avazu MDSP|IT | Rome Milan null | TIM WIND Vodafone | Phone:Alcatel Samsung Generic | Android:6.0 7.0 | Chrome Mobile\",\"__ov\":null,\"ta\":\"Mobidea\",\"pr1pr\":\"1.75\",\"minute\":\"27\",\"pr1nm\":\"17228 - Games - IT - 3G - Playoo Angry Fish\",\"cs\":\"Avazu MDSP\",\"pa\":\"purchase\",\"__istc\":false,\"__db\":null,\"__dc\":null,\"t\":\"event\",\"ti\":\"pumies75dfj7\",\"v\":\"1\",\"time\":\"1517794059508\",\"tr\":\"1.75\",\"__isb\":true}";
	static final JSONObject JSONOUTPUT = new JSONObject(STROUT);
	
			
	@Test
	public void testJsonReplaceToNull() {
		
		Pipeline pipeline = TestPipeline.create();
		
		PCollection<String> input = pipeline.apply(Create.of(JSONINPUT.toString())).setCoder(StringUtf8Coder.of());
		
		PCollection<String> output = input.apply(ParDo.of(new JsonValuesReplaceToNull()));
		
		DataflowAssert.that(output).containsInAnyOrder(JSONOUTPUT.toString());
		
		pipeline.run();
		
	}
}


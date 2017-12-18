package agency.akcom.mmg.sherlock.df;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;

import agency.akcom.mmg.sherlock.df.options.BigQueryTableOptions;
import agency.akcom.mmg.sherlock.df.options.PubsubTopicOptions;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.UserAgentType;
import net.sf.uadetector.service.UADetectorServiceFactory;

/**
 * Sets up and starts streaming pipeline.
 *
 * @throws IOException
 *             if there is a problem setting up resources
 */
public class SherlockPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(SherlockPipeline.class);
	
	/**
	 * Parse provided UserAgent string and add more related parameters to JSON
	 */
	static class UserAgentParser extends DoFn<String, String> {
		
		@Override
		public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
			JSONObject elementJSON = new JSONObject(c.element());
			
			try {
				String userAgent = elementJSON.getString("ua");
			
				if (userAgent != null && !userAgent.isEmpty()) {
					
					// Get an UserAgentStringParser and analyze the requesting client
					UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
					ReadableUserAgent agent = parser.parse(userAgent);
					
					elementJSON.put("__bf", agent.getName()); // browserFamily
					elementJSON.put("__bv", agent.getVersionNumber().toVersionString()); // browserVersion
					
					elementJSON.put("__of", agent.getOperatingSystem().getName()); // osFamily
					elementJSON.put("__ov", agent.getOperatingSystem().getVersionNumber().toVersionString()); // osVersion
					
					//elementJSON.put("__db", agent.getProducer()); // deviceBrand
					//elementJSON.put("__dm", agent.getFamily().getName()); // deviceModel
					elementJSON.put("__dc", agent.getDeviceCategory().getName()); // deviceCategory
					
					elementJSON.put("__isb", agent.getType().equals(UserAgentType.ROBOT)); // isBot
					elementJSON.put("__isec",agent.getType().equals(UserAgentType.EMAIL_CLIENT)); // isEmailClient
					
//					{"device.isBot","STRING", "NULLABLE","__isb",""}	,
//					{"device.isEmailClient","STRING", "NULLABLE","__isec",""}	,

					
//				       System.out.println("- - - - - - - - - - - - - - - - -");
//				        // type
//				        System.out.println("Browser type: " + agent.getType().getName());
//				        System.out.println("Browser name: " + agent.getName());
//				        VersionNumber browserVersion = agent.getVersionNumber();
//				        System.out.println("Browser version: " + browserVersion.toVersionString());
//				        System.out.println("Browser version major: " + browserVersion.getMajor());
//				        System.out.println("Browser version minor: " + browserVersion.getMinor());
//				        System.out.println("Browser version bug fix: " + browserVersion.getBugfix());
//				        System.out.println("Browser version extension: " + browserVersion.getExtension());
//				        System.out.println("Browser producer: " + agent.getProducer());
//
//				        // operating system
//				        OperatingSystem os = agent.getOperatingSystem();
//				        System.out.println("\nOS Name: " + os.getName());
//				        System.out.println("OS Producer: " + os.getProducer());
//				        VersionNumber osVersion = os.getVersionNumber();
//				        System.out.println("OS version: " + osVersion.toVersionString());
//				        System.out.println("OS version major: " + osVersion.getMajor());
//				        System.out.println("OS version minor: " + osVersion.getMinor());
//				        System.out.println("OS version bug fix: " + osVersion.getBugfix());
//				        System.out.println("OS version extension: " + osVersion.getExtension());
//
//				        // device category
//				        ReadableDeviceCategory device = agent.getDeviceCategory();
//				        System.out.println("\nDevice: " + device.getName());
					
//					{"device.ip","STRING", "NULLABLE","uip, __uip",""}	,


//					{"device.deviceBrand","STRING", "NULLABLE","__db",""}	,
//					{"device.deviceModel","STRING", "NULLABLE","__dm",""}	,
//					{"device.deviceCategory","STRING", "NULLABLE","__dc",""}	,

//					{"device.isTouchCapable","STRING", "NULLABLE","__istc",""}	,
//					{"device.isBot","STRING", "NULLABLE","__isb",""}	,
//					{"device.isEmailClient","STRING", "NULLABLE","__isec",""}	,
//					{"device.flashVersion","STRING", "NULLABLE","fl, __fl","ga:flashVersion"}	,
//					{"device.javaEnabled","INTEGER", "NULLABLE", "__je","ga:javaEnabled"}	, // 'je' - TODO need to implement converting to boolean
//					{"device.language","STRING", "NULLABLE","ul, __ul","ga:language"}	,
//					{"device.screenColors","STRING", "NULLABLE","sd, __sd","ga:screenColors"}	,
//					{"device.screenResolution","STRING", "NULLABLE","sr, __sr","ga:screenResolution"}	,
//					{"device.viewPort","STRING", "NULLABLE","__vp",""}	,
//					{"device.encoding","STRING", "NULLABLE","__enc",""}	,

				} else {
					LOG.error("Cann't find UserAget string ('ua')"); 
				}
			
			} catch (JSONException e) {
				LOG.error(e.getMessage());
			}
			
			LOG.info("UserAgentParser: " + elementJSON.toString());
					
			c.output(elementJSON.toString());
		}
	}
	
	/**
	 * Replace this generic label $$CUSTOM_PARAM(xxxxxxx)$$ (where xxxxx can by any value) with a null 
	 */
	static class JsonValuesCleaner extends DoFn<String, String> {
		@Override
		public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
			JSONObject elementJSON = new JSONObject(c.element());
			
			

			List<String> keysToDelete = new ArrayList<>();
			for (String key : elementJSON.keySet()) {
				String value = elementJSON.getString(key);
				if (value.startsWith("$$CUSTOM_PARAM(")) { // TODO improve matching

					LOG.warn(String.format("Removing key='%s' value='%s'", key, value));

					keysToDelete.add(key);
				}
			}

			for (String key : keysToDelete) {
				elementJSON.remove(key);
			}

			c.output(elementJSON.toString());
		}
	}
	
	/**
	 * Converts strings into BigQuery rows.
	 */
	static class StringToRowConverter extends DoFn<String, TableRow> {

		@Override
		public void processElement(ProcessContext c) throws IOException {
			// This document lists all of the parameters for the Measurement Protocol.
			// https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters
			TableRow tableRow = new TableRowCreator(c.element()).getTableRow();

			LOG.info(tableRow.toString());

			c.output(tableRow);
		}

		static TableSchema getSchema() {
			return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
				private String[] tmpSchemaRow;

				// Compose the list of TableFieldSchema from tableSchema.
				{
					// https://support.owox.com/hc/en-us/articles/217490677-Streaming-schema-for-hits
					addAll(getFieldShemas(DataflowUtils.SCHEMA_WITH_PARAMS.iterator(), null));
					add(new TableFieldSchema().setName("tmp_raw_request_json").setType("STRING"));
				}

				private List<TableFieldSchema> getFieldShemas(Iterator<String[]> schemaIterator, String recordName) {
					List<TableFieldSchema> fieldShemas = new ArrayList<TableFieldSchema>();
					String[] schemaRow;
					while (schemaIterator.hasNext()) {
						if (tmpSchemaRow != null) {
							schemaRow = tmpSchemaRow;
							tmpSchemaRow = null;
						} else {
							schemaRow = schemaIterator.next().clone();
							if (recordName != null) {
								if (schemaRow[0].startsWith(recordName + ".")) {
									schemaRow[0] = schemaRow[0].replaceFirst(recordName + ".", "");
								} else {
									tmpSchemaRow = schemaRow;
									return fieldShemas;
								}
							}
						}
						fieldShemas.add(getNextFieldShema(schemaIterator, schemaRow));
					}
					return fieldShemas;
				}

				private TableFieldSchema getNextFieldShema(Iterator<String[]> schemaIterator, String[] schemaRow) {
					if ("RECORD".equals(schemaRow[1])) {
						return new TableFieldSchema().setName(schemaRow[0]).setType(schemaRow[1]).setMode(schemaRow[2])
								.setFields(getFieldShemas(schemaIterator, schemaRow[0]));
					} else {
						return new TableFieldSchema().setName(schemaRow[0]).setType(schemaRow[1]).setMode(schemaRow[2]);
					}
				}
			});
		}
	}

	/**
	 * Options supported by {@link SherlockPipeline}.
	 *
	 * <p>
	 * Inherits standard configuration options.
	 */
	private interface SherlockOptions extends PubsubTopicOptions, BigQueryTableOptions {
	}

	public static void main(String[] args) throws IOException {
		SherlockOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SherlockOptions.class);
		options.setStreaming(true);
		// In order to cancel the pipelines automatically,
		// {@literal DataflowPipelineRunner} is forced to be used.
		// options.setRunner(DataflowPipelineRunner.class);
		options.setBigQuerySchema(StringToRowConverter.getSchema()); 
		options.setWorkerMachineType("n1-standard-1");
		options.setZone("europe-west3-a");
		options.setNumWorkers(0);

		DataflowUtils dataflowUtils = new DataflowUtils(options);
		dataflowUtils.setup();

		String tableSpec = new StringBuilder().append(options.getProject()).append(":")
				.append(options.getBigQueryDataset()).append(".").append(options.getBigQueryTable()).toString();

		Pipeline pipeline = Pipeline.create(options);

		pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()))
				.apply(ParDo.of(new JsonValuesCleaner()))
				.apply(ParDo.of(new UserAgentParser()))
				.apply(ParDo.of(new StringToRowConverter()))
				.apply(Window.<TableRow>into(CalendarWindows.days(1)))
				.apply(BigQueryIO.Write.withSchema(StringToRowConverter.getSchema())
						.to(new SerializableFunction<BoundedWindow, String>() {
							@Override
							public String apply(BoundedWindow window) {
								// The cast below is safe because CalendarWindows.days(1) produces
								// IntervalWindows.
								String dayString = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC)
										.print(((IntervalWindow) window).start());
								return tableSpec + "_" + dayString;
							}
						}).withWriteDisposition(WriteDisposition.WRITE_APPEND));

		pipeline.run();
	}
}

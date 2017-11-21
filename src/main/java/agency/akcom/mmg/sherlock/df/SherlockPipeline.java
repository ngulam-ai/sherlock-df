package agency.akcom.mmg.sherlock.df;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

import agency.akcom.mmg.sherlock.df.options.BigQueryTableOptions;
import agency.akcom.mmg.sherlock.df.options.PubsubTopicOptions;

/**
 * Sets up and starts streaming pipeline.
 *
 * @throws IOException
 *             if there is a problem setting up resources
 */
public class SherlockPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(SherlockPipeline.class);
	
	private static final List<String[]> SCHEMA_WITH_PARAMS = Arrays
			.asList(new String[][] { 
				{"hitId","STRING","hitId",""}	,
				{"userId","STRING","uid",""}	,
				{"userPhone","STRING","tel",""}	,
				{"userEmail","STRING","email",""}	,
//				{"userOwoxId","STRING","",""}	,
				{"clientId","STRING","","cid"}	,
				{"trackingId","STRING","tid",""}	,
				{"date","STRING","","ga:date"}	,
				
//				{"traffic","RECORD","",""}	,
//				{"traffic.referralPath","STRING","",""}	,
//				{"traffic.campaign","STRING","",""}	,
//				{"traffic.source","STRING","",""}	,
//				{"traffic.medium","STRING","",""}	,
//				{"traffic.keyword","STRING","",""}	,
//				{"traffic.adContent","STRING","",""}	,
//				{"traffic.campaignId","STRING","",""}	,
//				{"traffic.gclid","STRING","",""}	,
//				{"traffic.dclid","STRING","",""}	,
				
//				{"device","RECORD","",""}	,
//				{"device.ip","STRING","",""}	,
//				{"device.userAgent","STRING","",""}	,
//				{"device.flashVersion","STRING","",""}	,
//				{"device.javaEnabled","BOOLEAN","",""}	,
//				{"device.language","STRING","",""}	,
//				{"device.screenColors","STRING","",""}	,
//				{"device.screenResolution","STRING","",""}	,
				
//				{"geo","RECORD","",""}	,
//				{"geo.id","STRING","",""}	,
				
//				{"customDimensions","RECORD","",""}	,
//				{"customDimensions.index","INTEGER","",""}	,
//				{"customDimensions.value","STRING","",""}	,
				
//				{"customMetrics","RECORD","",""}	,
//				{"customMetrics.index","INTEGER","",""}	,
//				{"customMetrics.value","FLOAT","",""}	,
				
//				{"customGroups","RECORD","",""}	,
//				{"customGroups.index","INTEGER","",""}	,
//				{"customGroups.value","STRING","",""}	,
				{"hour","INTEGER","","ga:hour"}	,
				{"minute","INTEGER","","ga:minute"}	,
				{"time","INTEGER","time",""}	,
				{"queueTime","INTEGER","qt",""}	,
				{"isSecure","BOOLEAN","",""}	,
				{"isInteraction","BOOLEAN","ni",""}	,
				{"currency","STRING","","ga:currencyCode"}	,
				{"referer","STRING","","ga:fullReferrer"}	,
				{"dataSource","STRING","","ga:dataSource"}	,
				
//				{"social","RECORD","",""}	,
//				{"social.socialInteractionAction","STRING","",""}	,
//				{"social.socialInteractionNetwork","STRING","",""}	,
//				{"social.socialInteractionTarget","STRING","",""}	,
				
				{"type","STRING","t",""}	,
//				{"page","RECORD","",""}	,
//				{"page.pagePath","STRING","",""}	,
//				{"page.hostname","STRING","",""}	,
//				{"page.pageTitle","STRING","",""}	,
				
//				{"eCommerceAction","RECORD","",""}	,
//				{"eCommerceAction.action_type","STRING","",""}	,
//				{"eCommerceAction.option","STRING","",""}	,
//				{"eCommerceAction.step","INTEGER","",""}	,
//				{"eCommerceAction.list","STRING","",""}	,
				
//				{"experiment","RECORD","",""}	,
//				{"experiment.experimentId","STRING","",""}	,
//				{"experiment.experimentVariant","STRING","",""}	,
				
//				{"product","RECORD","",""}	,
//				{"product.isImpression","BOOLEAN","",""}	,
//				{"product.impressionList","STRING","",""}	,
//				{"product.productBrand","STRING","",""}	,
//				{"product.productPrice","FLOAT","",""}	,
//				{"product.productQuantity","INTEGER","",""}	,
//				{"product.productSku","STRING","",""}	,
//				{"product.productVariant","STRING","",""}	,
//				{"product.productCategory","STRING","",""}	,
//				{"product.productName","STRING","",""}	,
//				{"product.position","INTEGER","",""}	,
//				{"product.coupon","STRING","",""}	,
//				{"product.customDimensions","RECORD","",""}	,
//				{"product.customDimensions.index","INTEGER","",""}	,
//				{"product.customDimensions.value","STRING","",""}	,
//				{"product.customMetrics","RECORD","",""}	,
//				{"product.customMetrics.index","INTEGER","",""}	,
//				{"product.customMetrics.value","STRING","",""}	,
				
//				{"promotion","RECORD","",""}	,
//				{"promotion.promoCreative","STRING","",""}	,
//				{"promotion.promoId","STRING","",""}	,
//				{"promotion.promoName","STRING","",""}	,
//				{"promotion.promoPosition","STRING","",""}	,
				
				{"promotionActionInfo","STRING","promoa",""}	,
				
//				{"transaction","RECORD","",""}	,
//				{"transaction.transactionId","STRING","",""}	,
//				{"transaction.transactionRevenue","FLOAT","",""}	,
//				{"transaction.transactionTax","FLOAT","",""}	,
//				{"transaction.transactionShipping","FLOAT","",""}	,
//				{"transaction.transactionCoupon","STRING","",""}	,
//				{"transaction.affiliation","STRING","",""}	,
				
//				{"contentInfo","RECORD","",""}	,
//				{"contentInfo.contentDescription","STRING","",""}	,
				
//				{"appInfo","RECORD","",""}	,
//				{"appInfo.name","STRING","",""}	,
//				{"appInfo.version","STRING","",""}	,
//				{"appInfo.id","STRING","",""}	,
//				{"appInfo.installerId","STRING","",""}	,
				
//				{"exceptionInfo","RECORD","",""}	,
//				{"exceptionInfo.description","STRING","",""}	,
//				{"exceptionInfo.isFatal","BOOLEAN","",""}	,
				
//				{"eventInfo","RECORD","",""}	,
//				{"eventInfo.eventCategory","STRING","",""}	,
//				{"eventInfo.eventAction","STRING","",""}	,
//				{"eventInfo.eventLabel","STRING","",""}	,
//				{"eventInfo.eventValue","STRING","",""}	,
				
//				{"timingInfo","RECORD","",""}	,
//				{"timingInfo.timingCategory","STRING","",""}	,
//				{"timingInfo.timingVariable","STRING","",""}	,
//				{"timingInfo.timingLabel","STRING","",""}	,
//				{"timingInfo.timingValue","INTEGER","",""}	,
//				{"timingInfo.pageLoad","INTEGER","",""}	,
//				{"timingInfo.DNS","INTEGER","",""}	,
//				{"timingInfo.pageDownload","INTEGER","",""}	,
//				{"timingInfo.redirectResponse","INTEGER","",""}	,
//				{"timingInfo.TCPConnect","INTEGER","",""}	,
//				{"timingInfo.serverResponse","INTEGER","",""}	,
//				{"timingInfo.DOMInteractive","INTEGER","",""}	,
//				{"timingInfo.contentLoad","INTEGER","",""}	,
});

	/**
	 * Converts strings into BigQuery rows.
	 */
	static class StringToRowConverter extends DoFn<String, TableRow> {

		@Override
		public void processElement(ProcessContext c) {
			LOG.info("StringToRowConverter: " + c.element());			
			JSONObject elementJSON = new JSONObject(c.element());
			
			// This document lists all of the parameters for the Measurement Protocol.
			// https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters
			TableRow tableRow = new TableRow();
			for (String[] fieldSchema : SCHEMA_WITH_PARAMS) {
				String key = fieldSchema[2];
				if (!key.isEmpty()) {
					try {
						tableRow.set(fieldSchema[0], elementJSON.get(key));
					} catch (JSONException e) {
						LOG.warn(e.getMessage());
					}
				}
			}
			
			tableRow.set("tmp_raw_request_json", c.element());
			
			c.output(tableRow);

		}

		static TableSchema getSchema() {
			return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
				// Compose the list of TableFieldSchema from tableSchema.
				{
					// https://support.owox.com/hc/en-us/articles/217490677-Streaming-schema-for-hits
					for (String[] fieldSchema : SCHEMA_WITH_PARAMS) {
						add(new TableFieldSchema().setName(fieldSchema[0]).setType(fieldSchema[1]));
					}

					add(new TableFieldSchema().setName("tmp_raw_request_json").setType("STRING"));
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

		DataflowUtils dataflowUtils = new DataflowUtils(options);
		dataflowUtils.setup();

		String tableSpec = new StringBuilder().append(options.getProject()).append(":")
				.append(options.getBigQueryDataset()).append(".").append(options.getBigQueryTable()).toString();

		Pipeline pipeline = Pipeline.create(options);

		pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic())).apply(ParDo.of(new StringToRowConverter()))
				.apply(BigQueryIO.Write.to(tableSpec).withSchema(StringToRowConverter.getSchema())
						.withWriteDisposition(WriteDisposition.WRITE_APPEND));

		pipeline.run();
	}
}

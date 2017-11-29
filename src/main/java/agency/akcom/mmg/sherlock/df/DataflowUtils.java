package agency.akcom.mmg.sherlock.df;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.joda.time.Duration;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.util.FluentBackoff;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.common.collect.Lists;

import agency.akcom.mmg.sherlock.df.options.BigQueryTableOptions;
import agency.akcom.mmg.sherlock.df.options.PubsubTopicAndSubscriptionOptions;

public class DataflowUtils {
	
	static final List<String[]> SCHEMA_WITH_PARAMS = Arrays
			.asList(new String[][] { 
				{"hitId","STRING","hitId",""}	,
				{"userId","STRING","uid",""}	,
				{"userPhone","STRING","tel",""}	,
				{"userEmail","STRING","email",""}	,
//				{"userOwoxId","STRING","",""}	,
				{"clientId","STRING","","cid"}	,
				{"trackingId","STRING","tid",""}	,
				{"date","STRING","","ga:date"}	,
				
				{"traffic","RECORD","",""}	,
				{"traffic.referralPath","STRING","dr","ga:referralPath"}	,
				{"traffic.campaign","STRING","","ga:campaign"}	,
				{"traffic.source","STRING","","ga:source"}	,
				{"traffic.medium","STRING","","ga:medium"}	,
				{"traffic.keyword","STRING","","ga:keyword"}	,
				{"traffic.adContent","STRING","","ga:adContent"}	,
				{"traffic.campaignId","STRING","ci",""}	,
				{"traffic.gclid","STRING","gclid",""}	,
				{"traffic.dclid","STRING","dclid",""}	,
				
				{"device","RECORD","",""}	,
				{"device.ip","STRING","uip",""}	,
				{"device.userAgent","STRING","ua",""}	,
				{"device.flashVersion","STRING","fl","ga:flashVersion"}	,
//				{"device.javaEnabled","BOOLEAN","je","ga:javaEnabled"}	, //TODO need to implement converting to boolean
				{"device.language","STRING","","ga:language"}	,
				{"device.screenColors","STRING","sd","ga:screenColors"}	,
				{"device.screenResolution","STRING","sr","ga:screenResolution"}	,
				
				{"geo","RECORD","",""}	,
				{"geo.id","STRING","geoid",""}	,
				
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
//				{"isInteraction","BOOLEAN","ni",""}	, //TODO need to implement converting to boolean
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
				
				{"eCommerceAction","RECORD","",""}	,
				{"eCommerceAction.action_type","STRING","pa",""}	,
				{"eCommerceAction.option","STRING","col",""}	,
				{"eCommerceAction.step","INTEGER","cos",""}	,
				{"eCommerceAction.list","STRING","pal",""}	,
				
				{"experiment","RECORD","",""}	,
				{"experiment.experimentId","STRING","xid",""}	,
				{"experiment.experimentVariant","STRING","xvar",""}	,
				
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

	private final DataflowPipelineOptions options;
	private Bigquery bigQueryClient = null;
	private Pubsub pubsubClient = null;
	private Dataflow dataflowClient = null;
	private List<String> pendingMessages = Lists.newArrayList();

	public DataflowUtils(DataflowPipelineOptions options) {
		this.options = options;
	}

	/**
	 * Sets up external resources that are required by the project, such as Pub/Sub
	 * topics and BigQuery tables.
	 *
	 * @throws IOException
	 *             if there is a problem setting up the resources
	 */
	public void setup() throws IOException {
		Sleeper sleeper = Sleeper.DEFAULT;
		BackOff backOff = FluentBackoff.DEFAULT.withMaxRetries(3).withInitialBackoff(Duration.millis(200)).backoff();
		Throwable lastException = null;
		try {
			do {
				try {
					setupPubsub();
					setupBigQueryTable();
					return;
				} catch (GoogleJsonResponseException e) {
					lastException = e;
				}
			} while (BackOffUtils.next(sleeper, backOff));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			// Ignore InterruptedException
		}
		throw new RuntimeException(lastException);
	}

	/**
	 * Sets up the Google Cloud Pub/Sub topic.
	 *
	 * <p>
	 * If the topic doesn't exist, a new topic with the given name will be created.
	 *
	 * @throws IOException
	 *             if there is a problem setting up the Pub/Sub topic
	 */
	public void setupPubsub() throws IOException {
		PubsubTopicAndSubscriptionOptions pubsubOptions = options.as(PubsubTopicAndSubscriptionOptions.class);
		if (!pubsubOptions.getPubsubTopic().isEmpty()) {
			pendingMessages.add("**********************Set Up Pubsub************************");
			setupPubsubTopic(pubsubOptions.getPubsubTopic());
			pendingMessages.add("The Pub/Sub topic has been set up: " + pubsubOptions.getPubsubTopic());

//			if (!pubsubOptions.getPubsubSubscription().isEmpty()) {
//				setupPubsubSubscription(pubsubOptions.getPubsubTopic(), pubsubOptions.getPubsubSubscription());
//				pendingMessages
//						.add("The Pub/Sub subscription has been set up: " + pubsubOptions.getPubsubSubscription());
//			}
		}
	}

	/**
	 * Sets up the BigQuery table with the given schema.
	 *
	 * <p>
	 * If the table already exists, the schema has to match the given one.
	 * Otherwise, will throw a RuntimeException. If the table doesn't exist, a new
	 * table with the given schema will be created.
	 *
	 * @throws IOException
	 *             if there is a problem setting up the BigQuery table
	 */
	public void setupBigQueryTable() throws IOException {
		BigQueryTableOptions bigQueryTableOptions = options.as(BigQueryTableOptions.class);
		if (bigQueryTableOptions.getBigQueryDataset() != null && bigQueryTableOptions.getBigQueryTable() != null
				&& bigQueryTableOptions.getBigQuerySchema() != null) {
			pendingMessages.add("******************Set Up Big Query Table*******************");
			setupBigQueryTable(bigQueryTableOptions.getProject(), bigQueryTableOptions.getBigQueryDataset(),
					bigQueryTableOptions.getBigQueryTable(), bigQueryTableOptions.getBigQuerySchema());
			pendingMessages.add("The BigQuery table has been set up: " + bigQueryTableOptions.getProject() + ":"
					+ bigQueryTableOptions.getBigQueryDataset() + "." + bigQueryTableOptions.getBigQueryTable());
		}
	}

	private void setupPubsubTopic(String topic) throws IOException {
		if (pubsubClient == null) {
			pubsubClient = Transport.newPubsubClient(options).build();
		}
		if (executeNullIfNotFound(pubsubClient.projects().topics().get(topic)) == null) {
			pubsubClient.projects().topics().create(topic, new Topic().setName(topic)).execute();
		}
	}

	private void setupPubsubSubscription(String topic, String subscription) throws IOException {
		if (pubsubClient == null) {
			pubsubClient = Transport.newPubsubClient(options).build();
		}
		if (executeNullIfNotFound(pubsubClient.projects().subscriptions().get(subscription)) == null) {
			Subscription subInfo = new Subscription().setAckDeadlineSeconds(60).setTopic(topic);
			pubsubClient.projects().subscriptions().create(subscription, subInfo).execute();
		}
	}

	private void setupBigQueryTable(String projectId, String datasetId, String tableId, TableSchema schema)
			throws IOException {
		if (bigQueryClient == null) {
			bigQueryClient = Transport.newBigQueryClient(options.as(BigQueryOptions.class)).build();
		}

		Datasets datasetService = bigQueryClient.datasets();
		if (executeNullIfNotFound(datasetService.get(projectId, datasetId)) == null) {
			Dataset newDataset = new Dataset()
					.setDatasetReference(new DatasetReference().setProjectId(projectId).setDatasetId(datasetId));
			datasetService.insert(projectId, newDataset).execute();
		}

		Tables tableService = bigQueryClient.tables();
		Table table = executeNullIfNotFound(tableService.get(projectId, datasetId, tableId));
		if (table == null) {
			Table newTable = new Table().setSchema(schema).setTableReference(
					new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId));
			tableService.insert(projectId, datasetId, newTable).execute();
		} else if (!table.getSchema().equals(schema)) {
			throw new RuntimeException("Table exists and schemas do not match, expecting: " + schema.toPrettyString()
					+ ", actual: " + table.getSchema().toPrettyString());
		}
	}

	private static <T> T executeNullIfNotFound(AbstractGoogleClientRequest<T> request) throws IOException {
		try {
			return request.execute();
		} catch (GoogleJsonResponseException e) {
			if (e.getStatusCode() == HttpServletResponse.SC_NOT_FOUND) {
				return null;
			} else {
				throw e;
			}
		}
	}

}

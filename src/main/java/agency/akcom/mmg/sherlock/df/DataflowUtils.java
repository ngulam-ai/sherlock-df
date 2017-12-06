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
				{"hitId","STRING", "NULLABLE","hitId",""}	,
				{"userId","STRING", "NULLABLE","uid",""}	,
				{"userPhone","STRING", "NULLABLE","tel",""}	,
				{"userEmail","STRING", "NULLABLE","email",""}	,
//				{"userOwoxId","STRING", "NULLABLE","",""}	,
				{"clientId","STRING", "NULLABLE","cid",""}	,
				{"trackingId","STRING", "NULLABLE","tid",""}	,
				{"date","STRING", "NULLABLE","","ga:date"}	,
				
				{"traffic","RECORD", "NULLABLE","",""}	,
				{"traffic.referralPath","STRING", "NULLABLE","dr","ga:referralPath"}	,
				{"traffic.campaign","STRING", "NULLABLE","cn","ga:campaign"}	,
				{"traffic.source","STRING", "NULLABLE","cs","ga:source"}	,
				{"traffic.medium","STRING", "NULLABLE","cm","ga:medium"}	,
				{"traffic.keyword","STRING", "NULLABLE","ck","ga:keyword"}	,
				{"traffic.adContent","STRING", "NULLABLE","cc","ga:adContent"}	,
				{"traffic.campaignId","STRING", "NULLABLE","ci",""}	,
				{"traffic.gclid","STRING", "NULLABLE","gclid",""}	,
				{"traffic.dclid","STRING", "NULLABLE","dclid",""}	,
				
				{"device","RECORD", "NULLABLE","",""}	,
				{"device.ip","STRING", "NULLABLE","uip",""}	,
				{"device.userAgent","STRING", "NULLABLE","ua",""}	,
				{"device.flashVersion","STRING", "NULLABLE","fl","ga:flashVersion"}	,
				{"device.javaEnabled","INTEGER", "NULLABLE", "","ga:javaEnabled"}	, // 'je' - TODO need to implement converting to boolean
				{"device.language","STRING", "NULLABLE","ul","ga:language"}	,
				{"device.screenColors","STRING", "NULLABLE","sd","ga:screenColors"}	,
				{"device.screenResolution","STRING", "NULLABLE","sr","ga:screenResolution"}	,
				
				{"geo","RECORD", "NULLABLE","",""}	,
				{"geo.id","STRING", "NULLABLE","geoid",""}	,
				
				{"customDimensions","RECORD", "REPEATED","cd",""}	,
				{"customDimensions.index","INTEGER", "NULLABLE","",""}	,
				{"customDimensions.value","STRING", "NULLABLE","",""}	,
				
				{"customMetrics","RECORD", "NULLABLE","",""}	,
				{"customMetrics.index","INTEGER", "NULLABLE","",""}	,
				{"customMetrics.value","FLOAT", "NULLABLE","",""}	,
				
				{"customGroups","RECORD", "NULLABLE","",""}	,
				{"customGroups.index","INTEGER", "NULLABLE","",""}	,
				{"customGroups.value","STRING", "NULLABLE","",""}	,
				
				{"hour","INTEGER", "NULLABLE","","ga:hour"}	,
				{"minute","INTEGER", "NULLABLE","","ga:minute"}	,
				{"time","INTEGER", "NULLABLE","time",""}	,
				
				{"queueTime","INTEGER", "NULLABLE","qt",""}	,
				{"isSecure","BOOLEAN", "NULLABLE","",""}	,
				{"isInteraction","BOOLEAN", "NULLABLE","",""}	, //TODO 'ni' - need to implement converting to boolean
				{"currency","STRING", "NULLABLE","","ga:currencyCode"}	,
				{"referer","STRING", "NULLABLE","dr","ga:fullReferrer"}	,
				{"dataSource","STRING", "NULLABLE","","ga:dataSource"}	,
				
				{"social","RECORD", "NULLABLE","",""}	,
				{"social.socialInteractionAction","STRING", "NULLABLE","",""}	,
				{"social.socialInteractionNetwork","STRING", "NULLABLE","",""}	,
				{"social.socialInteractionTarget","STRING", "NULLABLE","",""}	,
				
				{"type","STRING", "NULLABLE","t",""}	,
				
				{"page","RECORD", "NULLABLE","",""}	,
				{"page.pagePath","STRING", "NULLABLE","dp",""}	,
				{"page.hostname","STRING", "NULLABLE","dh",""}	,
				{"page.pageTitle","STRING", "NULLABLE","dt",""}	,
				
				{"eCommerceAction","RECORD", "NULLABLE","",""}	,
				{"eCommerceAction.action_type","STRING", "NULLABLE","pa",""}	,
				{"eCommerceAction.option","STRING", "NULLABLE","col",""}	,
				{"eCommerceAction.step","INTEGER", "NULLABLE","cos",""}	,
				{"eCommerceAction.list","STRING", "NULLABLE","pal",""}	,
				
				{"experiment","RECORD", "NULLABLE","",""}	,
				{"experiment.experimentId","STRING", "NULLABLE","xid",""}	,
				{"experiment.experimentVariant","STRING", "NULLABLE","xvar",""}	,
				
				{"product","RECORD", "NULLABLE","",""}	,
				{"product.isImpression","BOOLEAN", "NULLABLE","",""}	,
				{"product.impressionList","STRING", "NULLABLE","",""}	,
				{"product.productBrand","STRING", "NULLABLE","",""}	,
				{"product.productPrice","FLOAT", "NULLABLE","",""}	,
				{"product.productQuantity","INTEGER", "NULLABLE","",""}	,
				{"product.productSku","STRING", "NULLABLE","",""}	,
				{"product.productVariant","STRING", "NULLABLE","",""}	,
				{"product.productCategory","STRING", "NULLABLE","",""}	,
				{"product.productName","STRING", "NULLABLE","",""}	,
				{"product.position","INTEGER", "NULLABLE","",""}	,
				{"product.coupon","STRING", "NULLABLE","",""}	,
//				{"product.customDimensions","RECORD", "NULLABLE","",""}	,
//				{"product.customDimensions.index","INTEGER", "NULLABLE","",""}	,
//				{"product.customDimensions.value","STRING", "NULLABLE","",""}	,
//				{"product.customMetrics","RECORD", "NULLABLE","",""}	,
//				{"product.customMetrics.index","INTEGER", "NULLABLE","",""}	,
//				{"product.customMetrics.value","STRING", "NULLABLE","",""}	,
				
				{"promotion","RECORD", "NULLABLE","",""}	,
				{"promotion.promoCreative","STRING", "NULLABLE","",""}	,
				{"promotion.promoId","STRING", "NULLABLE","",""}	,
				{"promotion.promoName","STRING", "NULLABLE","",""}	,
				{"promotion.promoPosition","STRING", "NULLABLE","",""}	,
				
				{"promotionActionInfo","STRING", "NULLABLE","promoa",""}	,
				
				{"transaction","RECORD", "NULLABLE","",""}	,
				{"transaction.transactionId","STRING", "NULLABLE","",""}	,
				{"transaction.transactionRevenue","FLOAT", "NULLABLE","",""}	,
				{"transaction.transactionTax","FLOAT", "NULLABLE","",""}	,
				{"transaction.transactionShipping","FLOAT", "NULLABLE","",""}	,
				{"transaction.transactionCoupon","STRING", "NULLABLE","",""}	,
				{"transaction.affiliation","STRING", "NULLABLE","",""}	,
				
				{"contentInfo","RECORD", "NULLABLE","",""}	,
				{"contentInfo.contentDescription","STRING", "NULLABLE","",""}	,
				
				{"appInfo","RECORD", "NULLABLE","",""}	,
				{"appInfo.name","STRING", "NULLABLE","",""}	,
				{"appInfo.version","STRING", "NULLABLE","",""}	,
				{"appInfo.id","STRING", "NULLABLE","",""}	,
				{"appInfo.installerId","STRING", "NULLABLE","",""}	,
				
				{"exceptionInfo","RECORD", "NULLABLE","",""}	,
				{"exceptionInfo.description","STRING", "NULLABLE","",""}	,
				{"exceptionInfo.isFatal","BOOLEAN", "NULLABLE","",""}	,
				
				{"eventInfo","RECORD", "NULLABLE","",""}	,
				{"eventInfo.eventCategory","STRING", "NULLABLE","ec",""}	,
				{"eventInfo.eventAction","STRING", "NULLABLE","ea",""}	,
				{"eventInfo.eventLabel","STRING", "NULLABLE","el",""}	,
				{"eventInfo.eventValue","STRING", "NULLABLE","ev",""}	,
				
				{"timingInfo","RECORD", "NULLABLE","",""}	,
				{"timingInfo.timingCategory","STRING", "NULLABLE","",""}	,
				{"timingInfo.timingVariable","STRING", "NULLABLE","",""}	,
				{"timingInfo.timingLabel","STRING", "NULLABLE","",""}	,
				{"timingInfo.timingValue","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.pageLoad","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.DNS","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.pageDownload","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.redirectResponse","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.TCPConnect","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.serverResponse","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.DOMInteractive","INTEGER", "NULLABLE","",""}	,
				{"timingInfo.contentLoad","INTEGER", "NULLABLE","",""}	,
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

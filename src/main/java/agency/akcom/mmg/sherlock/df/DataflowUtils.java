package agency.akcom.mmg.sherlock.df;

import java.io.IOException;
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

			if (!pubsubOptions.getPubsubSubscription().isEmpty()) {
				setupPubsubSubscription(pubsubOptions.getPubsubTopic(), pubsubOptions.getPubsubSubscription());
				pendingMessages
						.add("The Pub/Sub subscription has been set up: " + pubsubOptions.getPubsubSubscription());
			}
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

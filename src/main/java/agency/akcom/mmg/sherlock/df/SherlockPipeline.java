package agency.akcom.mmg.sherlock.df;

import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
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

	/**
	 * Converts strings into BigQuery rows.
	 */
	static class StringToRowConverter extends DoFn<String, TableRow> {

		@Override
		@Deprecated
		public void processElement(ProcessContext c) {
			LOG.info("StringToRowConverter: " + c.element());
			
			c.output(new TableRow().set("hitId", c.element()));
			// TODO add more fields or consider to use something else
		}

		static TableSchema getSchema() {
			return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
				// Compose the list of TableFieldSchema from tableSchema.
				{
					add(new TableFieldSchema().setName("hitId").setType("STRING"));
					add(new TableFieldSchema().setName("clientId").setType("STRING"));
					add(new TableFieldSchema().setName("date").setType("STRING"));
					// TODO add more fields or consider to use something else
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

		DataflowUtils dataflowUtils = new DataflowUtils(options);
		dataflowUtils.setup();

		String tableSpec = new StringBuilder().append(options.getProject()).append(":")
				.append(options.getBigQueryDataset()).append(".").append(options.getBigQueryTable()).toString();

		Pipeline pipeline = Pipeline.create(options);

		pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic())).apply(ParDo.of(new StringToRowConverter()))
				.apply(BigQueryIO.Write.to(tableSpec).withSchema(StringToRowConverter.getSchema()));

		pipeline.run();
	}
}

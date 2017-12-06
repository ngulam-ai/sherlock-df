package agency.akcom.mmg.sherlock.df;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
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
import com.google.cloud.dataflow.sdk.values.PCollection;

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
		options.setNumWorkers(0);

		DataflowUtils dataflowUtils = new DataflowUtils(options);
		dataflowUtils.setup();

		String tableSpec = new StringBuilder().append(options.getProject()).append(":")
				.append(options.getBigQueryDataset()).append(".").append(options.getBigQueryTable()).toString();

		Pipeline pipeline = Pipeline.create(options);

		pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic())).apply(ParDo.of(new StringToRowConverter()))
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

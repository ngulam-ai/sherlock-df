package agency.akcom.mmg.sherlock.df.options;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure BigQuery tables. The project defaults
 * to the project being used to run.
 */
public interface BigQueryTableOptions extends DataflowPipelineOptions {
	@Description("BigQuery dataset name")
	@Default.String("MMG_Streaming")
	String getBigQueryDataset();

	void setBigQueryDataset(String dataset);

	@Description("BigQuery table name")
	@Default.InstanceFactory(BigQueryTableFactory.class)
	String getBigQueryTable();

	void setBigQueryTable(String table);

	@Description("BigQuery table schema")
	TableSchema getBigQuerySchema();

	void setBigQuerySchema(TableSchema schema);

	/**
	 * Returns the job name as the default BigQuery table name.
	 */
	class BigQueryTableFactory implements DefaultValueFactory<String> {
		@Override
		public String create(PipelineOptions options) {
			return "streaming";
			//return options.as(DataflowPipelineOptions.class).getJobName().replace('-', '_');
		}
	}
}
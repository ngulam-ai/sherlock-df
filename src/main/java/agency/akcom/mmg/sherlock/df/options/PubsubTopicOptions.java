package agency.akcom.mmg.sherlock.df.options;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure Pub/Sub topic in Dataflow examples.
 */
public interface PubsubTopicOptions extends DataflowPipelineOptions {
	@Description("Pub/Sub topic")
	@Default.InstanceFactory(PubsubTopicFactory.class)
	String getPubsubTopic();

	void setPubsubTopic(String topic);

	/**
	 * Returns a default Pub/Sub topic based on the project and the job names.
	 */
	class PubsubTopicFactory implements DefaultValueFactory<String> {
		@Override
		public String create(PipelineOptions options) {
			DataflowPipelineOptions dataflowPipelineOptions = options.as(DataflowPipelineOptions.class);
			return "projects/" + dataflowPipelineOptions.getProject() + "/topics/" + "sherlock-real-time-ga-hit-data";
			// dataflowPipelineOptions.getJobName();
		}
	}
}
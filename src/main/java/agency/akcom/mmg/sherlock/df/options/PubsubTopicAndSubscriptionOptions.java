package agency.akcom.mmg.sherlock.df.options;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure Pub/Sub topic/subscription.
 */
public interface PubsubTopicAndSubscriptionOptions extends PubsubTopicOptions {
	@Description("Pub/Sub subscription")
	@Default.InstanceFactory(PubsubSubscriptionFactory.class)
	String getPubsubSubscription();

	void setPubsubSubscription(String subscription);

	/**
	 * Returns a default Pub/Sub subscription based on the project and the job
	 * names.
	 */
	class PubsubSubscriptionFactory implements DefaultValueFactory<String> {
		@Override
		public String create(PipelineOptions options) {
			DataflowPipelineOptions dataflowPipelineOptions = options.as(DataflowPipelineOptions.class);
			return "projects/" + dataflowPipelineOptions.getProject() + "/subscriptions/"
					+ dataflowPipelineOptions.getJobName();
		}
	}
}
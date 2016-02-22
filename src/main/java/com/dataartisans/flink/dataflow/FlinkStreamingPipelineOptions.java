/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import org.apache.flink.streaming.api.CheckpointingMode;

/**
 * Options specific for <b>streaming</b> jobs which can be used to configure a Flink PipelineRunner.
 */
public interface FlinkStreamingPipelineOptions extends FlinkPipelineOptions {

	// TODO: 2/22/16 WRITE THESE ONES


	/**
	 * Enables checkpointing for the streaming job. The distributed state of the streaming
	 * dataflow will be periodically snapshotted. In case of a failure, the streaming
	 * dataflow will be restarted from the latest completed checkpoint. This method selects
	 * {@link CheckpointingMode#EXACTLY_ONCE} guarantees.
	 *
	 * <p>The job draws checkpoints periodically, in the given interval. The state will be
	 * stored in the configured state backend.</p>
	 */
	@Description("The interval in .")
	@Default.Integer(-1)
	long getCheckpointingInterval();
	void setCheckpointingInterval(long interval);
}
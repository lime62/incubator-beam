package org.apache.beam.runners.hama;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

public class TestHamaRunner extends PipelineRunner<HamaRunnerResult> {
  private HamaRunner delegate;

  private TestHamaRunner(HamaPipelineOptions options) {
    this.delegate = HamaRunner.fromOptions(options);
  }

  public static TestHamaRunner fromOptions(HamaPipelineOptions options) {
    HamaPipelineOptions hamaOptions = PipelineOptionsValidator.validate(HamaPipelineOptions.class, options);
    return new TestHamaRunner(hamaOptions);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(PTransform<InputT, OutputT> transform, InputT input) {
    return delegate.apply(transform, input);
  }

  @Override
  public HamaRunnerResult run(Pipeline pipeline) {
    TestPipelineOptions testPipelineOptions = pipeline.getOptions().as(TestPipelineOptions.class);
    HamaRunnerResult result = delegate.run(pipeline);
    return result;
  }
}

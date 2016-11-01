
package org.apache.beam.runners.hama;


import org.apache.beam.runners.hama.translation.HamaPipelineTranslator;
import org.apache.beam.runners.hama.translation.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class HamaRunner extends PipelineRunner<HamaRunnerResult> {

  private static final Logger LOG = LoggerFactory.getLogger(HamaRunner.class);

  private final HamaPipelineOptions pipelineOptions;

  private HamaRunner(HamaPipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public static HamaRunner create(HamaPipelineOptions options) {
    return new HamaRunner(options);
  }

  public static HamaRunner fromOptions(PipelineOptions options) {
    HamaPipelineOptions hamaPipelineOptions = PipelineOptionsValidator.validate(HamaPipelineOptions.class, options);
    return new HamaRunner(hamaPipelineOptions);
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(PTransform<InputT, OutputT> transform, InputT input) {
    return super.apply(transform, input);
  }

  @Override
  public HamaRunnerResult run(Pipeline pipeline) {
    final TranslationContext translationContext;
    try {
      translationContext = new TranslationContext(pipelineOptions);
    } catch (IOException e) {
      e.printStackTrace();
      // todo : handle exception
      return null;
    }
    HamaPipelineTranslator translator = new HamaPipelineTranslator(translationContext);
    translator.translate(pipeline);
    try {
      translationContext.computeOutputs();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return translationContext;
  }
}
package org.apache.beam.runners.hama.translation;

import org.apache.beam.runners.hama.HamaPipelineOptions;
import org.apache.beam.runners.hama.HamaRunnerResult;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.POutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.*;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TranslationContext implements HamaRunnerResult {

  private AppliedPTransform<?, ?, ?> currentTransform;
  private final HamaPipelineOptions pipelineOptions;
  private HamaConfiguration conf;
  private BSPJob bsp;
  private List<Class<? extends Superstep>> supersteps;
  private InputFormat inputFormat;
  private OutputFormat outputFormat;

  private WritableComparable<?> mapInKey;
  private Writable              mapInVal;
  private WritableComparable<?> mapOutKey;
  private Writable mapOutVal;

  // todo : only debugging purpose
  private final Path TMP_OUTPUT = new Path("/tmp/pi-" + System.currentTimeMillis());
  private Path inputPath;

  public TranslationContext(HamaPipelineOptions pipelineOptions) throws IOException {
    this.conf = new HamaConfiguration();
    this.bsp = new BSPJob(conf, SuperstepBSP.class);
    this.supersteps = new ArrayList<>();
    this.pipelineOptions = pipelineOptions;
    this.inputPath = null;
  }

  public BSPJob getBsp() {
    return bsp;
  }

  // todo : add container for processing supersteps
  public void setInputPath(Path inputPath) {
    this.inputPath = inputPath;
  }

  public void addSuperstep(Class<? extends Superstep> superstep) {
    supersteps.add(superstep);
  }

  public void computeOutputs() throws InterruptedException, IOException, ClassNotFoundException {
    bsp.setSupersteps(supersteps.toArray(new Class[supersteps.size()]));
    bsp.setNumBspTask(5);
    bsp.setOutputPath(TMP_OUTPUT);
//    bsp.setPartitioner(SimpleHashPartitioner.class);
    bsp.waitForCompletion(true);
  }

  public void setCurrentTransform(TransformTreeNode node) {
    currentTransform = AppliedPTransform.of(node.getFullName(), node.getInput(), node.getOutput(), (PTransform) node.getTransform());
  }

  public <OutputT extends POutput> OutputT getOutput() {
    return (OutputT) getCurrentTransform().getOutput();
  }

  private AppliedPTransform<?, ?, ?> getCurrentTransform() {
//    checkArgument(currentTransform != null, "current transform not set");
    return currentTransform;
  }

  @Override
  public State getState() {
    return null;
  }

  @Override
  public State cancel() throws IOException {
    return null;
  }

  @Override
  public State waitUntilFinish(Duration duration) throws IOException, InterruptedException {
    return null;
  }

  @Override
  public State waitUntilFinish() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator) throws AggregatorRetrievalException {
    return null;
  }


}

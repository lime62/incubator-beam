package org.apache.beam.runners.hama.translation;

import org.apache.beam.runners.hama.translation.io.KVWritable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.util.ReflectionUtils;
import org.joda.time.Instant;

import java.io.IOException;

public class DoFnFunction extends Superstep<WritableComparable<?>, Writable, WritableComparable<?>, Writable, KVWritable> {

//  private final OldDoFn<KV, KV> fn;
  private OldDoFn<KV, KV> fn;

  public DoFnFunction() {
  }

  public DoFnFunction(OldDoFn<KV, KV> fn) {
    this.fn = fn;
  }

  @Override
  protected void compute(BSPPeer peer) throws IOException {

//    Text t = ReflectionUtils.newInstance(Text.class);
//    LongWritable l = ReflectionUtils.newInstance(LongWritable.class);
    // todo : just for testing
    KVWritable<Text, LongWritable> received;
    int i = 0;
    while ((received = (KVWritable<Text, LongWritable>) peer.getCurrentMessage()) != null) {
      System.out.println((i++) + " : " + received.getValue() + "-" + received.getKey());

    }
//    ProcCtxt ctxt = new ProcCtxt(fn);
//    try {
//      fn.startBundle(ctxt);
//      fn.processElement(ctxt);
//      fn.finishBundle(ctxt);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
  }

  private class ProcCtxt extends OldDoFn<KV, KV>.ProcessContext {

    public ProcCtxt(OldDoFn<KV, KV> fn) {
      fn.super();
    }


    @Override
    public KV element() {
      return null;
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return null;
    }

    @Override
    public Instant timestamp() {
      return null;
    }

    @Override
    public BoundedWindow window() {
      return null;
    }

    @Override
    public PaneInfo pane() {
      return null;
    }

    @Override
    public WindowingInternals<KV, KV> windowingInternals() {
      return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return null;
    }

    @Override
    public void output(KV output) {

    }

    @Override
    public void outputWithTimestamp(KV output, Instant timestamp) {

    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {

    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {

    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      return null;
    }
  }
}

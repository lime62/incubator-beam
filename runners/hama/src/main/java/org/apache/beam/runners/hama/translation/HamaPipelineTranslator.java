package org.apache.beam.runners.hama.translation;

import com.google.common.collect.Maps;
import net.bytebuddy.implementation.bind.annotation.Super;
import org.apache.beam.runners.hama.translation.io.HadoopIO;
import org.apache.beam.runners.hama.translation.io.KVWritable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Superstep;
import org.apache.hama.util.ReflectionUtils;

import java.io.IOException;
import java.util.Map;

public class HamaPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {
  private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS = Maps.newHashMap();
  private final TranslationContext translationContext;

  public HamaPipelineTranslator(TranslationContext translationContext) {
    this.translationContext = translationContext;
  }

  static {
//    TRANSLATORS.put(Create.Values.class, create());
//    TRANSLATORS.put(Read.Bounded.class, read());
    TRANSLATORS.put(TextIO.Read.Bound.class, readUTF());
    TRANSLATORS.put(HadoopIO.Read.Bound.class, readHadoop());
    TRANSLATORS.put(HadoopIO.Write.Bound.class, writeHadoop());
    TRANSLATORS.put(ParDo.Bound.class, parDo());
//    TRANSLATORS.put(Combine.PerKey.class, combinePerKey());
//    TRANSLATORS.put(GroupByKey.class, gbk());
  }

  private static TransformTranslator gbk() {
    return new TransformTranslator() {
      @Override
      public void translate(PTransform transform, TranslationContext context) {

      }
    };
  }

  private static TransformTranslator combinePerKey() {
    return new TransformTranslator() {
      @Override
      public void translate(PTransform transform, TranslationContext context) {

      }
    };
  }


  // todo : just for testing
  // extends superstepbsp and add this kind of code
  public static class TestSuperStep extends Superstep {

    @Override
    protected void compute(BSPPeer peer) throws IOException {
      Text t = ReflectionUtils.newInstance(Text.class);
      LongWritable l = ReflectionUtils.newInstance(LongWritable.class);
      long i = 0;
      while (peer.readNext(l, t)) {
        System.out.println(t.toString());
        l.set(i++);
        KVWritable<Text, LongWritable> kv = new KVWritable<>(t, l);
        peer.send(peer.getPeerName(0), kv);
        t = ReflectionUtils.newInstance(Text.class);
        l = ReflectionUtils.newInstance(LongWritable.class);
      }
    }
  }

  private static <InputT, OutputT> TransformTranslator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformTranslator<ParDo.Bound<InputT, OutputT>>() {
      @Override
      public void translate(final ParDo.Bound<InputT, OutputT> transform, TranslationContext context) {
        context.addSuperstep(TestSuperStep.class);
        DoFnFunction dofn = new DoFnFunction((OldDoFn<KV, KV>) transform.getFn());
        context.addSuperstep(dofn.getClass());
      }
    };
  }

  private static <T> TransformTranslator<Create.Values<T>> create() {
    return new TransformTranslator<Create.Values<T>>() {
      @Override
      public void translate(Create.Values<T> transform, TranslationContext context) {

      }
    };
  }

  private static <T> TransformTranslator<Read.Bounded<T>> read() {
    return new TransformTranslator<Read.Bounded<T>>() {
      @Override
      public void translate(Read.Bounded<T> transform, TranslationContext context) {
        String name = transform.getName();
        BoundedSource<T> source = transform.getSource();
        PCollection<T> output = context.getOutput();


        // todo : add source inputformat to context
      }
    };
  }

  private static <T> TransformTranslator<TextIO.Read.Bound<T>> readUTF() {
    return new TransformTranslator<TextIO.Read.Bound<T>>() {
      @Override
      public void translate(TextIO.Read.Bound<T> transform, TranslationContext context) {
        String path = transform.getFilepattern();
        Path p = new Path(path);
        context.setInputPath(p);
      }
    };
  }

  private static <K, V> TransformTranslator<HadoopIO.Read.Bound<K, V>> readHadoop() {
    return new TransformTranslator<HadoopIO.Read.Bound<K, V>>() {
      @Override
      public void translate(HadoopIO.Read.Bound<K, V> transform, TranslationContext context) {
        BSPJob bspJob = context.getBsp();
        bspJob.setInputValueClass(transform.getKeyClass());
        bspJob.setOutputValueClass(transform.getValueClass());
        bspJob.setInputFormat(transform.getFormatClass());
        bspJob.setInputPath(new Path(transform.getFilepattern()));
      }
    };
  }

  private static <K, V> TransformTranslator<HadoopIO.Write.Bound<K, V>> writeHadoop() {
    return new TransformTranslator<HadoopIO.Write.Bound<K, V>>() {
      @Override
      public void translate(HadoopIO.Write.Bound<K, V> transform, TranslationContext context) {
        BSPJob bspJob = context.getBsp();
        bspJob.setOutputFormat(transform.getFormatClass());
        bspJob.setOutputKeyClass(transform.getKeyClass());
        bspJob.setOutputValueClass(transform.getValueClass());
      }
    };
  }


  public void translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
    return super.enterCompositeTransform(node);
  }

  private <TransformT extends PTransform<?, ?>> TransformTranslator<TransformT> getTransformTranslator(Class<TransformT> clazz) {
    return TRANSLATORS.get(clazz);
  }

  @Override
  public void visitPrimitiveTransform(TransformTreeNode node) {
//    super.visitPrimitiveTransform(node);
    PTransform transform = node.getTransform();
    TransformTranslator translator = getTransformTranslator(transform.getClass());
    if (null == translator) {
      throw new IllegalStateException("no translator registered for " + transform);
    }
    translationContext.setCurrentTransform(node);
    translator.translate(transform, translationContext);
  }
}

package org.apache.beam.runners.hama.example;

import org.apache.beam.runners.hama.HamaPipelineOptions;
import org.apache.beam.runners.hama.HamaRunner;
import org.apache.beam.runners.hama.coders.WritableCoder;
import org.apache.beam.runners.hama.translation.io.HadoopIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.*;


public class WordCount {
  /**
   * Function to extract words.
   */
  public static class ExtractWordsFn extends DoFn<String, String> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", new Sum.SumLongFn());

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }


  /**
   * A SimpleFunction that converts a Word and Count into a printable string.
   */
  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }

  /**
   * PTransform counting words.
   */
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }

  public interface Options extends PipelineOptions, HamaPipelineOptions {
    @Description("Path of the file to read from")
    String getInput();

    void setInput(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }


  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(Options.class);
    options.setRunner(HamaRunner.class);

    Pipeline p = Pipeline.create(options);
   @SuppressWarnings("unchecked")
    Class<? extends FileInputFormat<Text, LongWritable>> inputFormatClass =
        (Class<? extends FileInputFormat<Text, LongWritable>>)
            (Class<?>) TextInputFormat.class;

    HadoopIO.Read.Bound<Text, LongWritable> read =
        HadoopIO.Read.from(options.getInput(),
            inputFormatClass,
            Text.class,
            LongWritable.class);

    PCollection<KV<Text, LongWritable>> input = p.apply(read)
        .setCoder(KvCoder.of(WritableCoder.of(Text.class), WritableCoder.of(LongWritable.class)));

    // just simple test to check if superstep in dofn can get data from previous superstep
    PCollection<KV<Text, LongWritable>> output = input.apply("test", ParDo.of(new DoFn<KV<Text, LongWritable>, KV<Text, LongWritable>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        for (String word : c.element().toString().split("[^a-zA-Z']+")) {
          if (!word.isEmpty()) {
            c.output(KV.of(new Text(word), new LongWritable(11)));
          }
        }
      }
    }));
//        .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    final Path TMP_OUTPUT = new Path("/tmp/pi-" + System.currentTimeMillis());
    @SuppressWarnings("unchecked")
    Class<? extends FileOutputFormat<Text, LongWritable>> outputFormatClass =
        (Class<? extends FileOutputFormat<Text, LongWritable>>)
            (Class<?>) TextOutputFormat.class;
    @SuppressWarnings("unchecked")
    HadoopIO.Write.Bound<Text, LongWritable> write = HadoopIO.Write.to(TMP_OUTPUT.getName(),
        outputFormatClass, Text.class, LongWritable.class);
    input.apply(write.withoutSharding());

//    p.apply("ReadLines", TextIO.Read.from(options.getInput()));
//        .apply(new CountWords())
//        .apply(MapElements.via(new FormatAsTextFn()))
//        .apply("WriteCounts", TextIO.Write.to(options.getOutput()));

    p.run();
  }
}

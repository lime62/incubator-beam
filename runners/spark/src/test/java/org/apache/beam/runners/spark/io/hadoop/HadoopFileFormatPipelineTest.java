/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.spark.io.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.coders.WritableCoder;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Pipeline on the Hadoop file format test.
 */
public class HadoopFileFormatPipelineTest {

  private File inputFile;
  private File outputFile;

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    inputFile = tmpDir.newFile("test.seq");
    outputFile = tmpDir.newFolder("out");
    outputFile.delete();
  }

  @Test
  public void testSequenceFile() throws Exception {
    populateFile();

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);
    @SuppressWarnings("unchecked")
    Class<? extends FileInputFormat<Text, LongWritable>> inputFormatClass =
        (Class<? extends FileInputFormat<Text, LongWritable>>)
            (Class<?>) SequenceFileInputFormat.class;

    HadoopIO.Read.Bound<Text, LongWritable> read =
        HadoopIO.Read.from(inputFile.getAbsolutePath(),
            inputFormatClass,
            Text.class,
            LongWritable.class);

    PCollection<KV<Text, LongWritable>> input = p.apply(read)
        .setCoder(KvCoder.of(WritableCoder.of(Text.class), WritableCoder.of(LongWritable.class)));


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

    @SuppressWarnings("unchecked")
    Class<? extends FileOutputFormat<Text, LongWritable>> outputFormatClass =
        (Class<? extends FileOutputFormat<Text, LongWritable>>)
            (Class<?>) TemplatedSequenceFileOutputFormat.class;
    @SuppressWarnings("unchecked")
    HadoopIO.Write.Bound<Text, LongWritable> write = HadoopIO.Write.to(outputFile.getAbsolutePath(),
        outputFormatClass, Text.class, LongWritable.class);
    input.apply(write.withoutSharding());
    EvaluationResult res = (EvaluationResult) p.run();
    res.close();

    IntWritable key = new IntWritable();
    Text value = new Text();
    try (Reader reader = new Reader(new Configuration(),
        Reader.file(new Path(outputFile.toURI())))) {
      int i = 0;
      while (reader.next(key, value)) {
        assertEquals(i, key.get());
        assertEquals("value-" + i, value.toString());
        i++;
      }
    }
  }

  private void populateFile() throws IOException {
    IntWritable key = new IntWritable();
    Text value = new Text();
    try (Writer writer = SequenceFile.createWriter(
        new Configuration(),
        Writer.keyClass(IntWritable.class), Writer.valueClass(Text.class),
        Writer.file(new Path(this.inputFile.toURI())))) {
      for (int i = 0; i < 5; i++) {
        key.set(i);
        value.set("value-" + i);
        writer.append(key, value);
      }
    }
  }

}

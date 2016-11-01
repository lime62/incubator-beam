package org.apache.beam.runners.hama.translation.io;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.RecordReader;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

// todo : test if it can be used for general input
public class SourceInputFormat<K, V extends WindowedValue> implements InputFormat<K, V> {
  private final BoundedSource<V> initialSource;
  private transient PipelineOptions options;
  private transient BoundedSource.BoundedReader<V> reader;
  private boolean inputAvailable = false;

  public SourceInputFormat(BoundedSource<V> initialSource, PipelineOptions options) throws IOException {
    this.initialSource = initialSource;
    this.options = options;
  }

  @Override
  public InputSplit[] getSplits(BSPJob bspJob, int numBspTask) throws IOException {
    try {
      long desiredSizeBytes = initialSource.getEstimatedSizeBytes(options) / numBspTask;
      List<? extends Source<V>> shards = initialSource.splitIntoBundles(desiredSizeBytes, options);
      int numShards = shards.size();
      SourceInputSplit[] sourceInputSplits = new SourceInputSplit[numShards];
      for (int i = 0; i < numShards; i++) {
        sourceInputSplits[i] = new SourceInputSplit(shards.get(i), i);
      }
      return sourceInputSplits;
    } catch (Exception e) {
      throw new IOException("Could not create input splits from Source.", e);
    }
  }

  private class SourceRecordReader<K, V> implements RecordReader<K, WindowedValue<V>> {

    private transient BoundedSource.BoundedReader<V> reader;

    public SourceRecordReader(SourceInputSplit<V> sourceInputSplit, BSPJob bspJob) throws IOException {

      reader = ((BoundedSource<V>) sourceInputSplit.getSource()).createReader(options);
      inputAvailable = reader.start();
    }

    @Override
    public boolean next(K key, WindowedValue<V> value) throws IOException {
      if (inputAvailable) {
        final V current = reader.getCurrent();
        final Instant timestamp = reader.getCurrentTimestamp();
        inputAvailable = reader.advance();
        value = WindowedValue.of(current, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
        return true;
      }
      return false;
    }


    @Override
    public K createKey() {
      return null;
    }

    @Override
    public WindowedValue<V> createValue() {
      return new WindowedValue<V>() {
        @Override
        public <NewT> WindowedValue<NewT> withValue(NewT value) {
          return null;
        }

        @Override
        public V getValue() {
          return null;
        }

        @Override
        public Instant getTimestamp() {
          return null;
        }

        @Override
        public Collection<? extends BoundedWindow> getWindows() {
          return null;
        }

        @Override
        public PaneInfo getPane() {
          return null;
        }

        @Override
        public String toString() {
          return null;
        }
      };
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  @Override
  public RecordReader getRecordReader(InputSplit inputSplit, BSPJob bspJob) throws IOException {
    return new SourceRecordReader<>((SourceInputSplit<V>) inputSplit, bspJob);
  }
}

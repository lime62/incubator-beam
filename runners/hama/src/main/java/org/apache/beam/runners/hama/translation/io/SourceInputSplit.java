package org.apache.beam.runners.hama.translation.io;

import org.apache.beam.sdk.io.Source;
import org.apache.hama.bsp.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SourceInputSplit<T> implements InputSplit {
  private Source<T> source;
  private int splitNumber;

  public SourceInputSplit(Source<T> source, int splitNumber) {
    this.source = source;
    this.splitNumber = splitNumber;
  }

  public Source<T> getSource() {
    return source;
  }

  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}

package org.apache.beam.runners.hama.translation;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPPeer;

import java.io.IOException;

public abstract class DataflowSuperstep <KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE extends Writable> {

  /**
   * Setup this superstep, is called once before compute().
   */
  protected void setup(BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer) {

  }

  /**
   * Cleanup this superstep, is called once after compute().
   */
  protected void cleanup(BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer) {

  }

  /**
   * Main computation phase.
   */
  protected abstract void compute(
      BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer)
      throws IOException;

  /**
   * @return true to halt the computation
   */
  protected boolean haltComputation(
      BSPPeer<KEYIN, VALUEIN, KEYOUT, VALUEOUT, MESSAGE> peer) {
    return false;
  }

}


package org.apache.beam.runners.hama.translation;

import org.apache.beam.sdk.transforms.PTransform;

import java.io.Serializable;

public interface TransformTranslator<T extends PTransform<?,?>> extends Serializable {
  void translate(T transform, TranslationContext context);
}

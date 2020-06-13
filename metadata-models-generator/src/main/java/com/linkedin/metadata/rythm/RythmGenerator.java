package com.linkedin.metadata.rythm;

import java.util.HashMap;
import java.util.Map;
import org.rythmengine.Rythm;
import org.rythmengine.conf.RythmConfigurationKey;


/**
 * Base class for code generator based on <a href="http://rythmengine.org/">rythm template engine</a>.
 */
public abstract class RythmGenerator {

  public void setupRythmEngine() {
    final Map<String, Object> config = new HashMap<>();
    StreamResourceLoader loader = new StreamResourceLoader();
    config.put(RythmConfigurationKey.CODEGEN_COMPACT_ENABLED.getKey(), false);
    config.put(RythmConfigurationKey.RESOURCE_LOADER_IMPLS.getKey(), loader);
    Rythm.init(config);
    loader.setEngine(Rythm.engine());
  }
}
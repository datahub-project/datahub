package com.linkedin.metadata.rythm;

import org.rythmengine.resource.ITemplateResource;
import org.rythmengine.resource.ResourceLoaderBase;


/**
 * Rythm template resource loader that can load template from resource stream under root directory `rythm`.
 */
public class StreamResourceLoader extends ResourceLoaderBase {
  @Override
  public String getResourceLoaderRoot() {
    return "rythm";
  }

  @Override
  public ITemplateResource load(String path) {
    return new StreamTemplateResource(path, this);
  }
}
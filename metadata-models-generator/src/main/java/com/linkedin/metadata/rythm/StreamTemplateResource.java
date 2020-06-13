package com.linkedin.metadata.rythm;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import org.apache.commons.io.IOUtils;
import org.rythmengine.resource.TemplateResourceBase;


/**
 * An implementation of {@link TemplateResourceBase} based on templates located
 * at resource stream located under root directory "rythm" at first.
 * Otherwise it will load template based on the relative path.
 */
public class StreamTemplateResource extends TemplateResourceBase {
  private final String path;
  private URLConnection connection = null;

  public StreamTemplateResource(String path, StreamResourceLoader loader) {
    super(loader);
    this.path = path;

    final URL url = getClass().getClassLoader().getResource(path);
    if (url != null) {
      try {
        connection = url.openConnection();
      } catch (IOException ex) {
        throw new RuntimeException("Get template resource failed", ex);
      }
    } else {
      try {
        final File rythm = new File(path);
        if (rythm.exists()) {
          connection = rythm.toURL().openConnection();
        }
      } catch (IOException ex) {
        throw new RuntimeException("Get template resource failed", ex);
      }
    }
  }

  @Override
  public Object getKey() {
    return path;
  }

  @Override
  public boolean isValid() {
    return connection != null;
  }

  @Override
  protected long defCheckInterval() {
    return 1000 * 5;
  }

  @Override
  protected long lastModified() {
    return connection.getLastModified();
  }

  @Override
  protected String reload() {
    if (isValid()) {
      try {
        return IOUtils.toString(connection.getInputStream());
      } catch (IOException e) {
        return null;
      }
    } else {
      return null;
    }
  }
}
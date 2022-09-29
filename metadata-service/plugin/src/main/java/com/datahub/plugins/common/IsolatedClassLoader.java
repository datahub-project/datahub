package com.datahub.plugins.common;

import com.datahub.plugins.Plugin;
import javax.annotation.Nonnull;


public interface IsolatedClassLoader {
  @Nonnull
  public Plugin instantiatePlugin(Class<? extends Plugin> expectedInstanceOf) throws ClassNotFoundException;
}

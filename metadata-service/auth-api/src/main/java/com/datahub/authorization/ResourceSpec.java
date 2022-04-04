package com.datahub.authorization;

import javax.annotation.Nonnull;
import lombok.Value;


@Value
public class ResourceSpec {
  @Nonnull
  String type;

  @Nonnull
  String resource;
}
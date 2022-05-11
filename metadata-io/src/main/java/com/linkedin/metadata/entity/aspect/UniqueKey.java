package com.linkedin.metadata.entity.aspect;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nonnull;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class UniqueKey {

  @Nonnull
  private String urn;

  @Nonnull
  private String aspect;

  @Nonnull
  private long version;
}

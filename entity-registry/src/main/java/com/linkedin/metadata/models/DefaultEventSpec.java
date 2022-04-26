package com.linkedin.metadata.models;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.annotation.EventAnnotation;
import lombok.Getter;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
@Getter
public class DefaultEventSpec implements EventSpec {
  private final String name;
  private final EventAnnotation eventAnnotation;
  private final RecordDataSchema pegasusSchema;
}

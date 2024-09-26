package com.linkedin.test.metadata.aspect.batch;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@Getter
@EqualsAndHashCode
public class TestSystemAspect implements SystemAspect {
  private Urn urn;
  private long version;
  private RecordTemplate recordTemplate;
  private SystemMetadata systemMetadata;
  private EntitySpec entitySpec;
  private AspectSpec aspectSpec;
  private Timestamp createdOn;
  private String createdBy;
}

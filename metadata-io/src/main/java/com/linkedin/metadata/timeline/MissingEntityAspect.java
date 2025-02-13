package com.linkedin.metadata.timeline;

import com.linkedin.metadata.aspect.EntityAspect;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = false)
public class MissingEntityAspect extends EntityAspect {}

package com.linkedin.metadata.entity;

public interface AspectDao {
    EntityAspect getAspect(String urn, String aspectName, long version);
}

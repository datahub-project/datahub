package com.linkedin.metadata.aspect.plugins.filter;

/** Describes whether a DAO read is serving a full aspect read or an existence check. */
public enum ReadIntent {
  READ,
  EXISTS
}

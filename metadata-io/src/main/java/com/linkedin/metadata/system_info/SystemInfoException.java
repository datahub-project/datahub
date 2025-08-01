package com.linkedin.metadata.system_info;

/** Exception thrown when system information collection fails */
public class SystemInfoException extends RuntimeException {

  public SystemInfoException(String message) {
    super(message);
  }

  public SystemInfoException(String message, Throwable cause) {
    super(message, cause);
  }
}

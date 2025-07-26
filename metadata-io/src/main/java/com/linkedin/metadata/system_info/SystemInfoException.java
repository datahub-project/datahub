package com.linkedin.metadata.system_info;

public class SystemInfoException extends RuntimeException {

  public SystemInfoException(String message) {
    super(message);
  }

  public SystemInfoException(String message, Throwable cause) {
    super(message, cause);
  }
}

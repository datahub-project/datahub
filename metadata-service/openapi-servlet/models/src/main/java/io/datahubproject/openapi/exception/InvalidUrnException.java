package io.datahubproject.openapi.exception;

import java.net.URISyntaxException;

public class InvalidUrnException extends URISyntaxException {
  public InvalidUrnException(String input, String reason) {
    super(input, reason);
  }
}

package datahub.event;

public class EventValidationException extends RuntimeException {
  public EventValidationException(String message) {
    super(message);
  }

  public EventValidationException(String message, Throwable t) {
    super(message, t);
  }
}
